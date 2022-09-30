package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3t "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqst "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	"github.com/gin-gonic/gin"
	"github.com/gobwas/glob"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	acceptHeader        = "Accept"
	cacheControlHeader  = "Cache-Control"
	contentLengthHeader = "Content-Length"
	contentTypeHeader   = "Content-Type"
	eTagHeader          = "ETag"
	lastModifiedHeader  = "Last-Modified"

	s3ErrCodeNotFound   = "NotFound"
	s3ErrCodeBadRequest = "BadRequest" // BadRequest occurs when key contains \ufffd.

	pathMetadata      = "original-path"
	timestampMetadata = "original-timestamp"

	APIVersion = 2
)

// Provided by govvv at compile time
var Version string

type configure struct {
	region                     string
	accessKeyID                string
	secretAccessKey            string
	s3Bucket                   string
	s3SrcPrefix                string
	s3DestPrefix               string
	sqsQueueURL                string
	sqsBatchWaitTime           uint
	efsMountPath               string
	temporaryCache             *cacheControl
	permanentCache             *cacheControl
	gracefulShutdownTimeout    uint
	port                       int
	logPath                    string
	errorLogPath               string
	pidFile                    string
	publicContentS3Bucket      string
	publicContentPathPatterns  string
	publicContentPathGlob      glob.Glob
	bypassMinifierPathPatterns string
	bypassMinifierPathGlob     glob.Glob
}

type cacheControl struct {
	name  string
	value string
}

// Environment holds values needed to execute the entire program.
type environment struct {
	configure
	awsConfig *aws.Config
	s3Client  *s3.Client
	sqsClient *sqs.Client
	log       *zap.Logger
}

// This implements zapcore.WriteSyncer interface.
type lockedFileWriteSyncer struct {
	m    sync.Mutex
	f    *os.File
	path string
}

func newLockedFileWriteSyncer(path string) *lockedFileWriteSyncer {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error while creating log file: path: %s", err.Error())
		panic(err)
	}

	return &lockedFileWriteSyncer{
		f:    f,
		path: path,
	}
}

func (s *lockedFileWriteSyncer) Write(bs []byte) (int, error) {
	s.m.Lock()
	defer s.m.Unlock()

	return s.f.Write(bs)
}

func (s *lockedFileWriteSyncer) Sync() error {
	s.m.Lock()
	defer s.m.Unlock()

	return s.f.Sync()
}

func (s *lockedFileWriteSyncer) reopen() {
	s.m.Lock()
	defer s.m.Unlock()

	if err := s.f.Close(); err != nil {
		fmt.Fprintf(
			os.Stderr, "error while reopening file: path: %s, err: %s", s.path, err.Error())
	}

	f, err := os.OpenFile(s.path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		fmt.Fprintf(
			os.Stderr, "error while reopening file: path: %s, err: %s", s.path, err.Error())
		panic(err)
	}

	s.f = f
}

func (s *lockedFileWriteSyncer) Close() error {
	s.m.Lock()
	defer s.m.Unlock()

	return s.f.Close()
}

func createLogger(ctx context.Context, logPath, errorLogPath string) *zap.Logger {
	enc := zapcore.NewJSONEncoder(zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        zapcore.OmitKey,
		CallerKey:      zapcore.OmitKey,
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "message",
		StacktraceKey:  zapcore.OmitKey,
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	})

	out := newLockedFileWriteSyncer(logPath)
	errOut := newLockedFileWriteSyncer(errorLogPath)

	sigusr1 := make(chan os.Signal, 1)
	signal.Notify(sigusr1, syscall.SIGUSR1)

	go func() {
		for {
			select {
			case _, ok := <-sigusr1:
				if !ok {
					return
				}
				out.reopen()
				errOut.reopen()
			case <-ctx.Done():
				signal.Stop(sigusr1)
				// closing sigusr1 causes panic (close of closed channel)
				return
			}
		}
	}()

	return zap.New(
		zapcore.NewCore(enc, out, zap.NewAtomicLevelAt(zap.DebugLevel)),
		zap.ErrorOutput(errOut),
		zap.Development(),
		zap.WithCaller(false)).With(zap.String("version", Version))
}

func createPathGlob(pattern string) glob.Glob {
	if pattern == "" {
		return nil
	}

	ps := strings.Split(pattern, ",")
	ps2 := make([]string, 0, len(ps))
	for _, p := range ps {
		ps2 = append(ps2, strings.TrimLeft(p, "/"))
	}

	g, err := glob.Compile("{" + strings.Join(ps2, ",") + "}")
	if err != nil {
		fmt.Fprintf(
			os.Stderr,
			"failed to compile glob: %s",
			err.Error())
		panic(err)
	}

	return g
}

func main() {
	app := cli.NewApp()
	app.Name = "imgserver"

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "region",
			Aliases: []string{"r"},
			Value:   "ap-northeast-1",
		},
		&cli.StringFlag{
			Name:     "s3-bucket",
			Aliases:  []string{"b"},
			Required: true,
		},
		&cli.StringFlag{
			Name:     "s3-src-prefix",
			Aliases:  []string{"sp"},
			Required: true,
		},
		&cli.StringFlag{
			Name:     "s3-dest-prefix",
			Aliases:  []string{"dp"},
			Required: true,
		},
		&cli.StringFlag{
			Name:     "sqs-queue-url",
			Aliases:  []string{"u"},
			Required: true,
		},
		&cli.UintFlag{
			Name:    "sqs-batch-wait-time",
			Aliases: []string{"w"},
			Value:   60,
		},
		&cli.StringFlag{
			Name:     "efs-mount-path",
			Aliases:  []string{"m"},
			Required: true,
		},
		&cli.UintFlag{
			Name:    "temp-resp-max-age",
			Aliases: []string{"ta"},
			Value:   20 * 60,
		},
		&cli.UintFlag{
			Name:    "perm-resp-max-age",
			Aliases: []string{"pa"},
			Value:   365 * 24 * 60 * 60,
		},
		&cli.UintFlag{
			Name:    "graceful-shutdown-timeout",
			Aliases: []string{"g"},
			Value:   5,
		},
		&cli.IntFlag{
			Name:    "port",
			Aliases: []string{"p"},
			Value:   8080,
		},
		&cli.StringFlag{
			Name:     "log-path",
			Aliases:  []string{"l"},
			Required: true,
		},
		&cli.StringFlag{
			Name:     "error-log-path",
			Aliases:  []string{"el"},
			Required: true,
		},
		&cli.StringFlag{
			Name:    "pid-file",
			Aliases: []string{"i"},
		},
		&cli.StringFlag{
			Name:        "public-content-s3-bucket",
			DefaultText: "",
			Aliases:     []string{"pubb"},
		},
		&cli.StringFlag{
			Name:        "public-content-path-patterns",
			DefaultText: "",
			Aliases:     []string{"pubp"},
		},
		&cli.StringFlag{
			Name:        "bypass-minifier-path-patterns",
			DefaultText: "",
			Aliases:     []string{"np"},
		},
	}

	app.Action = func(c *cli.Context) error {
		efsMouthPath, err := filepath.Abs(c.String("efs-mount-path"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get efs-mount-path: %s", err.Error())
			panic(err)
		}

		logPath, err := filepath.Abs(c.String("log-path"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get log-path: %s", err.Error())
			panic(err)
		}

		errorLogPath, err := filepath.Abs(c.String("error-log-path"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get error-log-path: %s", err.Error())
			panic(err)
		}

		cfg := &configure{
			region:                     c.String("region"),
			s3Bucket:                   c.String("s3-bucket"),
			s3SrcPrefix:                c.String("s3-src-prefix"),
			s3DestPrefix:               c.String("s3-dest-prefix"),
			sqsQueueURL:                c.String("sqs-queue-url"),
			sqsBatchWaitTime:           c.Uint("sqs-batch-wait-time"),
			efsMountPath:               efsMouthPath,
			gracefulShutdownTimeout:    c.Uint("graceful-shutdown-timeout"),
			port:                       c.Int("port"),
			logPath:                    logPath,
			errorLogPath:               errorLogPath,
			pidFile:                    c.String("pid-file"),
			publicContentS3Bucket:      c.String("public-content-s3-bucket"),
			publicContentPathPatterns:  c.String("public-content-path-patterns"),
			publicContentPathGlob:      createPathGlob(c.String("public-content-path-patterns")),
			bypassMinifierPathPatterns: c.String("bypass-minifier-path-patterns"),
			bypassMinifierPathGlob:     createPathGlob(c.String("bypass-minifier-path-patterns")),
			temporaryCache: &cacheControl{
				name:  "temporary",
				value: fmt.Sprintf("public, max-age=%d", c.Uint("temp-resp-max-age")),
			},
			permanentCache: &cacheControl{
				name:  "permanent",
				value: fmt.Sprintf("public, max-age=%d", c.Uint("perm-resp-max-age")),
			},
		}
		log := createLogger(c.Context, cfg.logPath, cfg.errorLogPath)
		defer func() {
			if err := log.Sync(); err != nil {
				fmt.Fprintf(os.Stderr, "failed to sync log on exiting: %s", err.Error())
			}
		}()

		if cfg.pidFile != "" {
			pid := []byte(strconv.Itoa(os.Getpid()))
			if err := os.WriteFile(cfg.pidFile, pid, 0644); err != nil {
				log.Panic(
					"failed to create PID file",
					zap.String("path", cfg.pidFile),
					zap.Error(err))
			}

			defer func() {
				if err := os.Remove(cfg.pidFile); err != nil {
					log.Error(
						"failed to remove PID file",
						zap.String("path", cfg.pidFile),
						zap.Error(err))
				}
			}()
		}

		gin.SetMode(gin.ReleaseMode)
		e := newEnvironment(c.Context, cfg, log)
		e.run(c.Context, e.runServer)

		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())

	sigkill := make(chan os.Signal, 1)
	signal.Notify(sigkill, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		<-sigkill
		signal.Stop(sigkill)
		close(sigkill)
		cancel()
	}()

	err := app.RunContext(ctx, os.Args)
	if err != nil {
		panic(err)
	}
}

type Location struct {
	Bucket string `json:"bucket"`
	Prefix string `json:"prefix"`
}

type task struct {
	Version int      `json:"version"`
	Path    string   `json:"path"`
	Src     Location `json:"src"`
	Dest    Location `json:"dest"`
}

type flushTimer interface {
	expiredCh() <-chan time.Time
	start()
	cancel()
}

type nilFlushTimer struct{}

func (t *nilFlushTimer) expiredCh() <-chan time.Time {
	return nil
}

func (t *nilFlushTimer) start() {
}

func (t *nilFlushTimer) cancel() {
}

type simpleFlushTimer struct {
	t *time.Timer
	d time.Duration
}

func newSimpleFlushTimer(d time.Duration) *simpleFlushTimer {
	timer := time.NewTimer(d)
	timer.Stop()

	return &simpleFlushTimer{
		t: timer,
		d: d,
	}
}

func (t *simpleFlushTimer) expiredCh() <-chan time.Time {
	return t.t.C
}

func (t *simpleFlushTimer) start() {
	if !t.t.Stop() {
		select {
		case <-t.t.C:
		default:
		}
	}
	t.t.Reset(t.d)
}

func (t *simpleFlushTimer) cancel() {
	if !t.t.Stop() {
		select {
		case <-t.t.C:
		default:
		}
	}
}

func createAWSConfig(ctx context.Context, cfg *configure) *aws.Config {
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.region))
	if err != nil {
		panic(err)
	}

	if cfg.accessKeyID != "" && cfg.secretAccessKey != "" {
		awsCfg.Credentials = credentials.NewStaticCredentialsProvider(
			cfg.accessKeyID, cfg.secretAccessKey, "")
	}
	return &awsCfg
}

func newEnvironment(ctx context.Context, cfg *configure, log *zap.Logger) *environment {
	awsConfig := createAWSConfig(ctx, cfg)
	return &environment{
		configure: *cfg,
		awsConfig: awsConfig,
		s3Client:  s3.NewFromConfig(*awsConfig),
		sqsClient: sqs.NewFromConfig(*awsConfig),
		log:       log,
	}
}

func (e *environment) runServer(ctx context.Context, engine *gin.Engine) {
	srv := &http.Server{
		Addr:    "127.0.0.1:" + strconv.Itoa(e.port),
		Handler: engine,
	}

	go func() {
		e.log.Info("server started")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			e.log.Panic("server finished abnormally", zap.Error(err))
		}
	}()

	<-ctx.Done()

	ctx2, cancel := context.WithTimeout(
		context.Background(), time.Duration(e.gracefulShutdownTimeout)*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx2); err != nil {
		e.log.Panic("server forced to shutdown", zap.Error(err))
	}
}

func (e *environment) run(ctx context.Context, runServer func(context.Context, *gin.Engine)) {
	taskCh := make(chan *task)

	taskSenderDone := make(chan struct{})
	defer func() {
		close(taskCh)
		<-taskSenderDone
	}()

	go e.taskSender(ctx, "tsend1", taskCh, taskSenderDone)

	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.GET("/*any", func(c *gin.Context) {
		e.handleRequest(c, c.Param("any"), c.GetHeader("Accept"), taskCh)
	})

	runServer(ctx, engine)

	e.log.Info("server exited")
}

func supportsWebP(acceptHeader string) bool {
	return strings.Contains(acceptHeader, "image/webp")
}

func (e *environment) sendTasks(
	ctx context.Context,
	entries []sqst.SendMessageBatchRequestEntry,
	tasks []*task,
) {
	res, err := e.sqsClient.SendMessageBatch(
		ctx,
		&sqs.SendMessageBatchInput{
			QueueUrl: &e.sqsQueueURL,
			Entries:  entries,
		})
	if err != nil {
		e.log.Error("error while sending tasks", zap.Error(err))
		return
	}

	for _, f := range res.Failed {
		var level func(string, ...zapcore.Field)
		if f.SenderFault {
			level = e.log.Error
		} else {
			level = e.log.Info
		}

		i, err := strconv.Atoi(*f.Id)
		if err != nil || i < 0 || len(entries) <= i {
			e.log.Error("unknown ID", zap.String("id", *f.Id))
			continue
		}

		level("failed to send task",
			zap.String("code", *f.Code),
			zap.String("message", *f.Message),
			zap.Bool("sender-fault", f.SenderFault),
			zap.String("path", tasks[i].Path),
		)
	}
}

func (e *environment) taskSender(ctx context.Context, id string, taskCh <-chan *task, done chan<- struct{}) {
	defer func() {
		done <- struct{}{}
	}()

	entries := make([]sqst.SendMessageBatchRequestEntry, 0, 10)
	tasks := make([]*task, 0, 10)

	// Never reuse allocated slice since task sender goroutine is still referencing it.
	clearBuf := func() {
		entries = make([]sqst.SendMessageBatchRequestEntry, 0, 10)
		tasks = make([]*task, 0, 10)
	}

	wg := &sync.WaitGroup{}
	defer wg.Wait()

	idField := zap.String("id", id)

	sendTaskAsync := func(entries []sqst.SendMessageBatchRequestEntry, tasks []*task) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.sendTasks(ctx, entries, tasks)
		}()
		clearBuf()
	}

	var flusher flushTimer
	if e.sqsBatchWaitTime == 0 {
		flusher = &nilFlushTimer{}
	} else {
		flusher = newSimpleFlushTimer(time.Duration(e.sqsBatchWaitTime) * time.Second)
	}
	defer flusher.cancel()

	for {
		select {
		case <-ctx.Done():
			if 0 < len(entries) {
				e.log.Info("abandoned tasks",
					idField,
					zap.Error(ctx.Err()),
					zap.Int("tasks", len(entries)))
				e.log.Debug("done by ctx", idField)
			}
			return
		case <-flusher.expiredCh():
			e.log.Debug("specified period elapsed; send tasks",
				idField,
				zap.Int("num", len(entries)),
				zap.Uint("period", e.sqsBatchWaitTime))
			sendTaskAsync(entries, tasks)
		case t, ok := <-taskCh:
			if !ok {
				e.log.Debug("input closed", idField)
				if 0 < len(entries) {
					e.log.Debug("send remaining tasks", idField, zap.Int("num", len(entries)))
					e.sendTasks(ctx, entries, tasks)
				}
				e.log.Debug("task sender done", idField)
				return
			}

			e.log.Debug("got new task", idField, zap.String("path", t.Path))
			id := strconv.Itoa(len(entries))
			jbs, err := json.Marshal(t)
			if err != nil {
				e.log.Error("failed to marshal JSON",
					idField,
					zap.String("path", t.Path),
					zap.Error(err))
				continue
			}

			entries = append(entries, sqst.SendMessageBatchRequestEntry{
				Id:          &id,
				MessageBody: aws.String(string(jbs)),
			})
			tasks = append(tasks, t)

			if len(entries) == 1 {
				flusher.start()
				continue
			}

			if len(entries) == 10 {
				e.log.Debug("buffer full; send tasks", idField, zap.Int("num", 10))
				sendTaskAsync(entries, tasks)
				flusher.cancel()
			}
		}
	}
}

type efsFileStatus struct {
	time *time.Time
	eTag string
	size int64
	err  error
}

type readSeekCloser interface {
	io.ReadSeeker
	io.Closer
}

type efsFileReader struct {
	reader readSeekCloser
	err    error
}

func (r *efsFileReader) Close() error {
	if r.reader != nil {
		return r.reader.Close()
	}
	return nil
}

func (r *efsFileReader) Seek(offset int64, whence int) (int64, error) {
	if r.reader != nil {
		return r.reader.Seek(offset, whence)
	}
	return 0, nil
}

type offset int

const (
	offsetStart = iota
	offsetEnd
	offsetUncontrolled
)

type efsFileBody struct {
	size       int64
	currOffset offset
	body       readSeekCloser
	log        *zap.Logger
}

func (b *efsFileBody) Read(p []byte) (n int, err error) {
	return b.body.Read(p)
}

func (b *efsFileBody) Close() error {
	if b.body != nil {
		return b.body.Close()
	}
	return nil
}

func (b *efsFileBody) Seek(offset int64, whence int) (int64, error) {
	seek := func() (int64, error) {
		b.currOffset = offsetUncontrolled
		return b.body.Seek(offset, whence)
	}

	switch b.currOffset {
	case offsetStart:
		switch whence {
		case io.SeekStart, io.SeekCurrent:
			return seek()
		case io.SeekEnd:
			b.currOffset = offsetEnd
			return b.size, nil
		default:
			b.log.Panic("should not be called")
		}

	case offsetEnd:
		switch whence {
		case io.SeekStart:
			if offset == 0 {
				b.currOffset = offsetStart
				return 0, nil
			}
			return seek()
		case io.SeekCurrent, io.SeekEnd:
			return seek()
		default:
			b.log.Panic("should not be called")
		}

	case offsetUncontrolled:
		return seek()
	}

	b.log.Panic("should not be called")
	return 0, nil
}

type s3Body struct {
	size       int64
	pos        int64
	currOffset offset
	body       io.ReadCloser
	log        *zap.Logger
}

func (b *s3Body) Read(p []byte) (n int, err error) {
	switch b.currOffset {
	case offsetStart, offsetUncontrolled:
		b.currOffset = offsetUncontrolled
		n, err := b.body.Read(p)
		b.pos += int64(n)
		return n, err
	case offsetEnd:
		return 0, fmt.Errorf(
			"reading when offset is end is not permitted in this implementation")
	}
	b.log.Panic("should not be called")
	return 0, nil
}

func (b *s3Body) Close() error {
	if b.body != nil {
		return b.body.Close()
	}
	return nil
}

func (b *s3Body) Seek(offset int64, whence int) (int64, error) {
	cueForward := func(offset int64) (int64, error) {
		b.log.Debug("cueForward called",
			zap.Int64("offset", offset),
			zap.Int("whence", whence),
			zap.Int64("size", b.size),
			zap.Int64("pos", b.pos))
		b.currOffset = offsetUncontrolled

		if offset < 0 {
			return 0, fmt.Errorf(
				"negative seek offset is not permitted in this implementation: %d",
				offset)
		}

		const bufsize = 4096
		buf := make([]byte, bufsize)
		for remaining := offset; 0 < remaining; {
			if remaining < int64(len(buf)) {
				buf = buf[:remaining]
			}

			l, err := b.body.Read(buf)
			b.pos += int64(l)
			remaining -= int64(l)
			if err != nil {
				if err == io.EOF {
					return offset - remaining, nil
				}
				return 0, err
			}
		}

		return offset, nil
	}

	switch b.currOffset {
	case offsetStart:
		switch whence {
		case io.SeekStart, io.SeekCurrent:
			if offset == 0 {
				return 0, nil
			}
			return cueForward(offset)
		case io.SeekEnd:
			b.currOffset = offsetEnd
			return b.size, nil
		default:
			b.log.Panic("should not be called")
		}

	case offsetEnd:
		switch whence {
		case io.SeekStart:
			if offset == 0 {
				b.currOffset = offsetStart
				return 0, nil
			}
			return cueForward(offset)
		case io.SeekCurrent, io.SeekEnd:
			return cueForward(b.size + offset)
		default:
			b.log.Panic("should not be called")
		}

	case offsetUncontrolled:
		switch whence {
		case io.SeekStart:
			return cueForward(offset - b.pos)
		case io.SeekCurrent:
			return cueForward(offset)
		case io.SeekEnd:
			return cueForward(b.size + offset)
		default:
			b.log.Panic("should not be called")
		}
	}

	b.log.Panic("should not be called")
	return 0, nil
}

type s3ObjStatus struct {
	time *time.Time
	eTag string
	err  error
}

type s3ObjData struct {
	s3ObjStatus
	contentLength int64
	reader        *s3Body
}

func (d *s3ObjData) Close() error {
	if d.reader != nil {
		return d.reader.Close()
	}
	return nil
}

func quoteETag(eTag string) string {
	return "\"" + eTag + "\""
}

func (e *environment) checkEFSFileStatus(efsPath string) *efsFileStatusFuture {
	statusCh := make(chan *efsFileStatus)
	fut := &efsFileStatusFuture{
		ch: statusCh,
	}

	go func() {
		defer close(statusCh)

		zapPathField := zap.String("path", efsPath)

		stat, err := os.Stat(efsPath)
		if err != nil {
			// ENOTDIR error occurs when the path starts with an existing file path.
			if os.IsNotExist(err) ||
				errors.Is(err, syscall.ENOTDIR) ||
				errors.Is(err, syscall.ENAMETOOLONG) {

				e.log.Debug("stat: file not found", zapPathField)
				statusCh <- &efsFileStatus{}
				return
			}
			e.log.Error("failed to stat file", zapPathField, zap.Error(err))
			statusCh <- &efsFileStatus{err: err}
			return
		}

		s := stat.Size()
		t := stat.ModTime().UTC()
		eTag := quoteETag(fmt.Sprintf("%x-%x", t.UnixNano(), s))

		statusCh <- &efsFileStatus{
			time: &t,
			eTag: eTag,
			size: s,
		}
	}()

	return fut
}

func (e *environment) checkDestS3ObjStatus(ctx context.Context, s3Key string) *s3ObjStatusFuture {
	statusCh := make(chan *s3ObjStatus)
	fut := &s3ObjStatusFuture{
		ch: statusCh,
	}

	go func() {
		defer close(statusCh)

		s3KeyField := zap.String("key", s3Key)

		res, err := e.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: &e.s3Bucket,
			Key:    &s3Key,
		})
		if err != nil {
			var apiErr smithy.APIError
			if errors.As(err, &apiErr) {
				switch apiErr.ErrorCode() {
				case s3ErrCodeNotFound:
					statusCh <- &s3ObjStatus{}
				case s3ErrCodeBadRequest:
					e.log.Info("key contains invalid chars",
						s3KeyField,
						zap.String("aws-code", apiErr.ErrorCode()),
						zap.String("aws-message", apiErr.ErrorMessage()))
					statusCh <- &s3ObjStatus{}
				default:
					e.log.Error("failed to HEAD object",
						s3KeyField,
						zap.String("aws-code", apiErr.ErrorCode()),
						zap.String("aws-message", apiErr.ErrorMessage()))
					statusCh <- &s3ObjStatus{err: err}
				}

				return
			}

			e.log.Error("failed to connect to AWS", s3KeyField, zap.Error(err))
			statusCh <- &s3ObjStatus{err: err}
			return
		}

		if ts := res.Metadata[timestampMetadata]; ts != "" {
			t, err := time.Parse(time.RFC3339Nano, ts)
			if err != nil {
				e.log.Error("illegal timestamp", s3KeyField, zap.String("timestamp", ts))
				statusCh <- &s3ObjStatus{err: err}
				return
			}

			statusCh <- &s3ObjStatus{
				time: &t,
				eTag: *res.ETag,
			}
			return
		}

		e.log.Error("timestamp not found", s3KeyField)
		statusCh <- &s3ObjStatus{
			err: fmt.Errorf("no timestamp: %s", s3Key),
		}
	}()

	return fut
}

func (e *environment) getEFSFileReader(ctx context.Context, efsPath string) *efsFileReaderFuture {
	readerCh := make(chan *efsFileReader)
	fut := &efsFileReaderFuture{
		ch: readerCh,
	}

	go func() {
		defer close(readerCh)

		zapPathField := zap.String("path", efsPath)

		f, err := os.Open(efsPath)
		if err != nil {
			// ENOTDIR error occurs when the path starts with an existing file path.
			if os.IsNotExist(err) ||
				errors.Is(err, syscall.ENOTDIR) ||
				errors.Is(err, syscall.ENAMETOOLONG) {

				e.log.Debug("open: file not found", zapPathField)
				readerCh <- &efsFileReader{}
				return
			}
			e.log.Error("failed to open file", zapPathField, zap.Error(err))
			readerCh <- &efsFileReader{err: err}
			return
		}

		readerCh <- &efsFileReader{reader: f}
	}()

	return fut
}

func (e *environment) getDestS3ObjReader(ctx context.Context, s3Key string) *s3ObjDataFuture {
	readerCh := make(chan *s3ObjData)
	fut := &s3ObjDataFuture{
		ch: readerCh,
	}

	go func() {
		defer close(readerCh)

		s3KeyField := zap.String("key", s3Key)

		res, err := e.s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: &e.s3Bucket,
			Key:    &s3Key,
		})
		if err != nil {
			var noSuchKeyError *s3t.NoSuchKey
			if errors.As(err, &noSuchKeyError) {
				readerCh <- &s3ObjData{}
				return
			}

			var apiErr smithy.APIError
			if errors.As(err, &apiErr) {
				e.log.Error("failed to GET object",
					s3KeyField,
					zap.String("aws-code", apiErr.ErrorCode()),
					zap.String("aws-message", apiErr.ErrorMessage()))
				readerCh <- &s3ObjData{
					s3ObjStatus: s3ObjStatus{err: err},
				}
				return
			}

			e.log.Error("failed to connect to AWS", s3KeyField, zap.Error(err))
			readerCh <- &s3ObjData{
				s3ObjStatus: s3ObjStatus{err: err},
			}
			return
		}

		if ts := res.Metadata[timestampMetadata]; ts != "" {
			t, err := time.Parse(time.RFC3339Nano, ts)
			if err != nil {
				e.log.Error("illegal timestamp", s3KeyField, zap.String("timestamp", ts))
				readerCh <- &s3ObjData{
					s3ObjStatus: s3ObjStatus{err: err},
				}
				return
			}

			readerCh <- &s3ObjData{
				s3ObjStatus: s3ObjStatus{
					eTag: *res.ETag,
					time: &t,
				},
				contentLength: res.ContentLength,
				reader: &s3Body{
					body: res.Body,
					size: res.ContentLength,
					log:  e.log,
				},
			}
			return
		}

		e.log.Error("timestamp not found", s3KeyField)
		readerCh <- &s3ObjData{
			s3ObjStatus: s3ObjStatus{err: fmt.Errorf("no timestamp: %s", s3Key)},
		}
	}()

	return fut
}

type filePath struct {
	efs       string
	s3SrcKey  string
	s3DestKey string
	sqs       string
	path      string
	name      string
}

type efsFileStatusFuture struct {
	data *efsFileStatus
	ch   <-chan *efsFileStatus
}

func (f *efsFileStatusFuture) get() *efsFileStatus {
	if f.data == nil {
		f.data = <-f.ch
	}
	return f.data
}

type s3ObjStatusFuture struct {
	data *s3ObjStatus
	ch   <-chan *s3ObjStatus
}

func (f *s3ObjStatusFuture) get() *s3ObjStatus {
	if f.data == nil {
		f.data = <-f.ch
	}
	return f.data
}

type efsFileReaderFuture struct {
	data *efsFileReader
	ch   <-chan *efsFileReader
}

func (f *efsFileReaderFuture) get() *efsFileReader {
	if f.data == nil {
		f.data = <-f.ch
	}
	return f.data
}

type s3ObjDataFuture struct {
	data *s3ObjData
	ch   <-chan *s3ObjData
}

func (f *s3ObjDataFuture) get() *s3ObjData {
	if f.data == nil {
		f.data = <-f.ch
	}
	return f.data
}

func isEFSNull(readerFut *efsFileReaderFuture, statusFut *efsFileStatusFuture) bool {
	return readerFut.get().reader == nil || statusFut.get().time == nil
}

func hasErrorInEFS(readerFut *efsFileReaderFuture, statusFut *efsFileStatusFuture) bool {
	return readerFut.get().err != nil || statusFut.get().err != nil
}

func timeExistsAndEqual(
	fileStatusFut *efsFileStatusFuture,
	objReaderFut *s3ObjDataFuture,
) bool {
	return objReaderFut.get().time != nil &&
		fileStatusFut.get().time != nil &&
		objReaderFut.get().time.Equal(*fileStatusFut.get().time)
}

func (e *environment) ensureDestS3ObjUpdated(
	ctx context.Context,
	fileStatusFut *efsFileStatusFuture,
	destStatusFut *s3ObjStatusFuture,
	fileReaderFut *efsFileReaderFuture,
	fpath *filePath,
	taskCh chan<- *task,
) {
	zapPathField := zap.String("path", fpath.efs)

	//
	// The principle is that never change file if error occurs.
	//

	if fileStatusFut.get().err != nil || destStatusFut.get().err != nil {
		return
	}

	// Do nothing since the both files don't exist.
	if fileStatusFut.get().time == nil && destStatusFut.get().time == nil {
		return
	}

	if fileStatusFut.get().time != nil &&
		destStatusFut.get().time != nil &&
		fileStatusFut.get().time.Equal(*destStatusFut.get().time) {
		return
	}

	if fileReaderFut == nil {
		fileReaderFut = e.getEFSFileReader(ctx, fpath.efs)
		defer func() {
			if err := fileReaderFut.get().Close(); err != nil {
				e.log.Error("failed to close EFS file", zapPathField, zap.Error(err))
			}
		}()
	} else {
		fileReaderFut.get().Seek(0, io.SeekStart)
	}

	if fileReaderFut.get().err != nil {
		return
	}

	if !isEFSNull(fileReaderFut, fileStatusFut) {
		tsStr := fileStatusFut.get().time.Format(time.RFC3339Nano)
		if _, err := e.s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      &e.s3Bucket,
			Key:         &fpath.s3SrcKey,
			Body:        fileReaderFut.get().reader,
			ContentType: aws.String(e.contentType(fpath.s3SrcKey)),
			Metadata: map[string]string{
				pathMetadata:      fpath.path,
				timestampMetadata: tsStr,
			},
		}); err != nil {
			e.log.Error("failed to PUT S3 source object",
				zapPathField, zap.Error(err), zap.String("s3key", fpath.s3SrcKey))
			return
		}
	} else {
		if _, err := e.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: &e.s3Bucket,
			Key:    &fpath.s3SrcKey,
		}); err != nil {
			e.log.Error("failed to DELETE S3 source object", zapPathField, zap.Error(err))
			return
		}
	}

	select {
	case taskCh <- &task{
		Version: APIVersion,
		Path:    fpath.sqs,
		Src: Location{
			Bucket: e.s3Bucket,
			Prefix: e.s3SrcPrefix,
		},
		Dest: Location{
			Bucket: e.s3Bucket,
			Prefix: e.s3DestPrefix,
		},
	}:
	case <-ctx.Done():
	}
}

func (e *environment) logResponse(resType, path, source string, cache *cacheControl) {
	e.log.Debug("response",
		zap.String("type", resType),
		zap.String("path", path),
		zap.String("source", source),
		zap.String("cache", cache.name))
}

func (e *environment) respondWithInternalServerErrorText(c *gin.Context, fpath *filePath) {
	e.logResponse("500", fpath.path, "inline", e.temporaryCache)
	const message = "internal server error"
	c.Writer.Header().Set(cacheControlHeader, e.temporaryCache.value)
	c.Writer.Header().Set(contentLengthHeader, strconv.Itoa(len(message)))
	c.String(http.StatusInternalServerError, message)
	c.Writer.Flush()
}

func (e *environment) respondWithNotFoundText(c *gin.Context, fpath *filePath) {
	e.logResponse("404", fpath.path, "inline", e.temporaryCache)
	const message = "file not found"
	c.Writer.Header().Set(cacheControlHeader, e.temporaryCache.value)
	c.Writer.Header().Set(contentLengthHeader, strconv.Itoa(len(message)))
	c.String(http.StatusNotFound, message)
	c.Writer.Flush()
}

func (e *environment) respondWithS3Object(c *gin.Context, fpath *filePath, respData *s3ObjData) {
	e.logResponse("s3object", fpath.path, fpath.s3DestKey, e.permanentCache)
	c.Writer.Header().Set(cacheControlHeader, e.permanentCache.value)
	c.Writer.Header().Set(eTagHeader, respData.eTag)
	c.Writer.Header().Set(contentTypeHeader, e.contentType(fpath.s3DestKey))
	http.ServeContent(c.Writer, c.Request, "", *respData.time, respData.reader)
	c.Writer.Flush()
}

func (e *environment) respondWithPublicContentS3Object(
	c *gin.Context,
	fpath *filePath,
	res *s3.GetObjectOutput,
) {
	body := &s3Body{
		body: res.Body,
		size: res.ContentLength,
		log:  e.log,
	}
	defer func() {
		if err := body.Close(); err != nil {
			e.log.Error("failed to close public content s3 body",
				zap.Error(err),
				zap.String("path", fpath.path))
		}
	}()

	var cacheContol string
	if res.CacheControl == nil {
		cacheContol = "no-store, max-age=0"
	} else {
		cacheContol = *res.CacheControl
	}
	e.logResponse("s3pub", fpath.path, fpath.path, &cacheControl{
		name:  "s3",
		value: cacheContol,
	})
	c.Writer.Header().Set(cacheControlHeader, cacheContol)
	c.Writer.Header().Set(eTagHeader, *res.ETag)
	c.Writer.Header().Set(contentTypeHeader, e.contentType(fpath.path))
	http.ServeContent(c.Writer, c.Request, "", *res.LastModified, body)
	c.Writer.Flush()
}

func (e *environment) contentType(path string) string {
	ct := mime.TypeByExtension(strings.ToLower(filepath.Ext(path)))
	if ct == "" {
		return "application/octet-stream"
	}
	return ct
}

func (e *environment) respondWithEFSFile(
	c *gin.Context,
	fpath *filePath,
	status *efsFileStatus,
	reader readSeekCloser,
	cache *cacheControl,
) {
	e.logResponse("efsfile", fpath.path, fpath.efs, cache)

	body := &efsFileBody{
		body: reader,
		size: status.size,
		log:  e.log,
	}

	c.Writer.Header().Set(cacheControlHeader, cache.value)
	c.Writer.Header().Set(eTagHeader, status.eTag)
	c.Writer.Header().Set(contentTypeHeader, e.contentType(fpath.name))
	http.ServeContent(c.Writer, c.Request, fpath.name, *status.time, body)
	c.Writer.Flush()
}

func (e *environment) respondWithOriginalWhileGeneration(
	c *gin.Context,
	fpath *filePath,
	taskCh chan<- *task,
) {
	fileStatusFut := e.checkEFSFileStatus(fpath.efs)
	fileReaderFut := e.getEFSFileReader(c, fpath.efs)
	destStatusFut := e.checkDestS3ObjStatus(c, fpath.s3DestKey)
	defer func() {
		// Ensure each goroutine is finished.
		fileStatusFut.get()
		if err := fileReaderFut.get().Close(); err != nil {
			e.log.Error("failed to close EFS file",
				zap.Error(err),
				zap.String("path", fpath.efs))
		}
		destStatusFut.get()
	}()

	if hasErrorInEFS(fileReaderFut, fileStatusFut) {
		e.log.Error("failed to get file on EFS",
			zap.String("path", fpath.efs),
			zap.NamedError("readerErr", fileReaderFut.get().err),
			zap.NamedError("statusErr", fileStatusFut.get().err))
		e.respondWithInternalServerErrorText(c, fpath)
	} else if isEFSNull(fileReaderFut, fileStatusFut) {
		e.respondWithNotFoundText(c, fpath)
	} else {
		e.respondWithEFSFile(
			c, fpath, fileStatusFut.get(), fileReaderFut.get().reader, e.permanentCache)
	}

	e.ensureDestS3ObjUpdated(c, fileStatusFut, destStatusFut, fileReaderFut, fpath, taskCh)
}

func (e *environment) respondTemporarily(
	c *gin.Context,
	fpath *filePath,
	statusFut *efsFileStatusFuture,
	readerFut *efsFileReaderFuture,
) {
	if hasErrorInEFS(readerFut, statusFut) {
		e.respondWithInternalServerErrorText(c, fpath)
		e.log.Error("failed to get file on EFS",
			zap.String("path", fpath.efs),
			zap.NamedError("readerErr", readerFut.get().err),
			zap.NamedError("statusErr", statusFut.get().err))
		return
	}

	if isEFSNull(readerFut, statusFut) {
		e.respondWithNotFoundText(c, fpath)
		return
	}

	e.respondWithEFSFile(
		c, fpath, statusFut.get(), readerFut.get().reader, e.temporaryCache)
}

func (e *environment) respondWithGeneratedOrOriginalWhileGeneration(
	c *gin.Context,
	fpath *filePath,
	taskCh chan<- *task,
) {
	destReaderFut := e.getDestS3ObjReader(c, fpath.s3DestKey)
	fileStatusFut := e.checkEFSFileStatus(fpath.efs)
	defer func() {
		// Ensure each goroutine is finished.
		if err := destReaderFut.get().Close(); err != nil {
			e.log.Error("failed to close object body",
				zap.Error(err), zap.String("path", fpath.s3DestKey))
		}
		fileStatusFut.get()
	}()

	var fileReaderFut *efsFileReaderFuture
	defer func() {
		if fileReaderFut == nil {
			return
		}
		if err := fileReaderFut.get().Close(); err != nil {
			e.log.Error("failed to close EFS file", zap.Error(err), zap.String("path", fpath.efs))
		}
	}()

	if destReaderFut.get().err != nil {
		e.log.Info(
			"Generated file is requested but the original one will be responded due to error on S3",
			zap.String("path", fpath.efs),
			zap.Error(destReaderFut.get().err))
		fileReaderFut = e.getEFSFileReader(c, fpath.efs)
		e.respondTemporarily(c, fpath, fileStatusFut, fileReaderFut)
	} else if timeExistsAndEqual(fileStatusFut, destReaderFut) {
		e.respondWithS3Object(c, fpath, destReaderFut.get())
	} else {
		fileReaderFut = e.getEFSFileReader(c, fpath.efs)
		e.respondTemporarily(c, fpath, fileStatusFut, fileReaderFut)
	}

	e.ensureDestS3ObjUpdated(
		c,
		fileStatusFut,
		&s3ObjStatusFuture{data: &destReaderFut.get().s3ObjStatus},
		fileReaderFut,
		fpath,
		taskCh,
	)
}

func (e *environment) respondWithOriginalOrGenerated(
	c *gin.Context,
	fpath *filePath,
	taskCh chan<- *task,
) {
	fileStatusFut := e.checkEFSFileStatus(fpath.efs)
	fileReaderFut := e.getEFSFileReader(c, fpath.efs)
	defer func() {
		// Ensure each goroutine is finished.
		if err := fileReaderFut.get().Close(); err != nil {
			e.log.Error("failed to close EFS file",
				zap.Error(err),
				zap.String("path", fpath.efs))
		}
		fileStatusFut.get()
	}()

	if hasErrorInEFS(fileReaderFut, fileStatusFut) {
		e.log.Error("failed to get file on EFS",
			zap.String("path", fpath.efs),
			zap.NamedError("readerErr", fileReaderFut.get().err),
			zap.NamedError("statusErr", fileStatusFut.get().err))
		e.respondWithInternalServerErrorText(c, fpath)
		return
	}

	if !isEFSNull(fileReaderFut, fileStatusFut) {
		e.respondWithEFSFile(
			c, fpath, fileStatusFut.get(), fileReaderFut.get().reader, e.permanentCache)
		return
	}

	destReaderFut := e.getDestS3ObjReader(c, fpath.s3DestKey)
	defer func() {
		if err := destReaderFut.get().Close(); err != nil {
			e.log.Error("failed to close object body",
				zap.Error(err),
				zap.String("path", fpath.s3DestKey))
		}
	}()

	if destReaderFut.get().err != nil {
		e.log.Error("failed to GET S3 object",
			zap.String("path", fpath.s3DestKey),
			zap.Error(destReaderFut.get().err))
		e.respondWithInternalServerErrorText(c, fpath)
		return
	}

	if destReaderFut.get().reader == nil {
		e.respondWithNotFoundText(c, fpath)
		return
	}

	e.respondWithS3Object(c, fpath, destReaderFut.get())
}

func (e *environment) respondWithOriginal(
	c *gin.Context,
	fpath *filePath,
	taskCh chan<- *task,
) {
	fileStatusFut := e.checkEFSFileStatus(fpath.efs)
	fileReaderFut := e.getEFSFileReader(c, fpath.efs)
	defer func() {
		// Ensure each goroutine is finished.
		if err := fileReaderFut.get().Close(); err != nil {
			e.log.Error("failed to close EFS file",
				zap.Error(err),
				zap.String("path", fpath.efs))
		}
		fileStatusFut.get()
	}()

	if hasErrorInEFS(fileReaderFut, fileStatusFut) {
		e.log.Error("failed to get file on EFS",
			zap.String("path", fpath.efs),
			zap.NamedError("readerErr", fileReaderFut.get().err),
			zap.NamedError("statusErr", fileStatusFut.get().err))
		e.respondWithInternalServerErrorText(c, fpath)
		return
	}

	if isEFSNull(fileReaderFut, fileStatusFut) {
		e.respondWithNotFoundText(c, fpath)
		return
	}

	e.respondWithEFSFile(
		c, fpath, fileStatusFut.get(), fileReaderFut.get().reader, e.permanentCache)
}

func (e *environment) respondWithPublicContentBucket(c *gin.Context, fpath *filePath) {
	s3Key := fpath.path
	s3KeyField := zap.String("key", s3Key)

	res, err := e.s3Client.GetObject(c, &s3.GetObjectInput{
		Bucket: &e.publicContentS3Bucket,
		Key:    &s3Key,
	})
	if err != nil {
		var noSuchKeyError *s3t.NoSuchKey
		if errors.As(err, &noSuchKeyError) {
			e.respondWithNotFoundText(c, fpath)
			return
		}

		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			e.log.Error("failed to GET object",
				s3KeyField,
				zap.String("aws-code", apiErr.ErrorCode()),
				zap.String("aws-message", apiErr.ErrorMessage()))
			e.respondWithInternalServerErrorText(c, fpath)
			return
		}

		e.log.Error("failed to connect to AWS", s3KeyField, zap.Error(err))
		e.respondWithInternalServerErrorText(c, fpath)
		return
	}

	e.respondWithPublicContentS3Object(c, fpath, res)
}

func (e *environment) handleRequest(
	c *gin.Context,
	path string,
	acceptHeader string,
	taskCh chan<- *task,
) {
	// path value contains leading "/".

	getNormalizedExtension := func(name string) string {
		n := strings.ToLower(name)
		extExt := []string{
			".js.map",
			".min.js",
			".min.css",
		}

		for _, e := range extExt {
			if strings.HasSuffix(n, e) {
				return e
			}
		}

		return filepath.Ext(n)
	}

	// Sanitize and reject malicious path
	efsAbsPath := filepath.Clean(filepath.Join(e.efsMountPath, path))
	if !strings.HasPrefix(efsAbsPath, e.efsMountPath+"/") {
		c.String(http.StatusBadRequest, "invalid URL path")
		return
	}
	sanitizedPath := strings.TrimPrefix(efsAbsPath, e.efsMountPath+"/")
	_, name := filepath.Split(sanitizedPath)

	fpath := &filePath{
		efs:  efsAbsPath,
		sqs:  sanitizedPath,
		path: sanitizedPath,
		name: name,
	}

	if e.publicContentPathGlob != nil && e.publicContentPathGlob.Match(fpath.path) {
		e.respondWithPublicContentBucket(c, fpath)
		return
	}

	if e.bypassMinifierPathGlob != nil && e.bypassMinifierPathGlob.Match(fpath.path) {
		e.respondWithOriginal(c, fpath, taskCh)
		return
	}

	switch getNormalizedExtension(name) {
	case ".jpg", ".jpeg", ".png", ".gif":
		fpath.s3SrcKey = e.s3SrcPrefix + sanitizedPath
		fpath.s3DestKey = e.s3DestPrefix + sanitizedPath + ".webp"
		if supportsWebP(acceptHeader) {
			e.respondWithGeneratedOrOriginalWhileGeneration(c, fpath, taskCh)
		} else {
			e.respondWithOriginalWhileGeneration(c, fpath, taskCh)
		}
	case ".js":
		fpath.s3SrcKey = e.s3SrcPrefix + sanitizedPath
		fpath.s3DestKey = e.s3DestPrefix + sanitizedPath
		e.respondWithGeneratedOrOriginalWhileGeneration(c, fpath, taskCh)
	case ".js.map":
		fpath.s3DestKey = e.s3DestPrefix + sanitizedPath
		e.respondWithOriginalOrGenerated(c, fpath, taskCh)
	case ".css":
		fpath.s3SrcKey = e.s3SrcPrefix + sanitizedPath
		fpath.s3DestKey = e.s3DestPrefix + sanitizedPath
		e.respondWithGeneratedOrOriginalWhileGeneration(c, fpath, taskCh)
	case ".min.js", ".min.css":
		e.respondWithOriginal(c, fpath, taskCh)
	default:
		e.log.Warn("unknown file type", zap.String("path", path))
		e.respondWithOriginal(c, fpath, taskCh)
	}
}
