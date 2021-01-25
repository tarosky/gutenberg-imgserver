package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/gin-gonic/gin"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	jpegContentType  = "image/jpeg"
	pngContentType   = "image/png"
	webPContentType  = "image/webp"
	plainContentType = "text/plain; charset=utf-8"

	acceptHeader        = "Accept"
	cacheControlHeader  = "Cache-Control"
	contentLengthHeader = "Content-Length"
	contentTypeHeader   = "Content-Type"
	eTagHeader          = "ETag"
	lastModifiedHeader  = "Last-Modified"

	s3ErrCodeNotFound = "NotFound" // https://forums.aws.amazon.com/thread.jspa?threadID=145909

	timestampMetadata = "Original-Timestamp"
	pathMetadata      = "Original-Path"
)

type config struct {
	region                  string
	accessKeyID             string
	secretAccessKey         string
	s3Bucket                string
	s3SrcKeyBase            string
	s3DestKeyBase           string
	sqsQueueURL             string
	sqsBatchWaitTime        uint
	efsMountPath            string
	temporaryCache          string
	permanentCache          string
	gracefulShutdownTimeout uint
	port                    int
	logPath                 string
	errorLogPath            string
	pidFile                 string
}

// Environment holds values needed to execute the entire program.
type environment struct {
	config
	awsSession *session.Session
	s3Client   *s3.S3
	sqsClient  *sqs.SQS
	log        *zap.Logger
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
					break
				}
				out.reopen()
				errOut.reopen()
			case <-ctx.Done():
				signal.Stop(sigusr1)
				// closing sigusr1 causes panic (close of closed channel)
				break
			}
		}
	}()

	return zap.New(
		zapcore.NewCore(enc, out, zap.NewAtomicLevelAt(zap.DebugLevel)),
		zap.ErrorOutput(errOut),
		zap.Development(),
		zap.WithCaller(false))
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
			Name:     "s3-src-key-base",
			Aliases:  []string{"sk"},
			Required: true,
		},
		&cli.StringFlag{
			Name:     "s3-dest-key-base",
			Aliases:  []string{"dk"},
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

		cfg := &config{
			region:                  c.String("region"),
			s3Bucket:                c.String("s3-bucket"),
			s3SrcKeyBase:            c.String("s3-src-key-base"),
			s3DestKeyBase:           c.String("s3-dest-key-base"),
			sqsQueueURL:             c.String("sqs-queue-url"),
			sqsBatchWaitTime:        c.Uint("sqs-batch-wait-time"),
			efsMountPath:            efsMouthPath,
			temporaryCache:          fmt.Sprintf("public, max-age=%d", c.Uint("temp-resp-max-age")),
			permanentCache:          fmt.Sprintf("public, max-age=%d", c.Uint("perm-resp-max-age")),
			gracefulShutdownTimeout: c.Uint("graceful-shutdown-timeout"),
			port:                    c.Int("port"),
			logPath:                 logPath,
			errorLogPath:            errorLogPath,
			pidFile:                 c.String("pid-file"),
		}
		log := createLogger(c.Context, cfg.logPath, cfg.errorLogPath)
		defer log.Sync()

		if cfg.pidFile != "" {
			pid := []byte(strconv.Itoa(os.Getpid()))
			if err := ioutil.WriteFile(cfg.pidFile, pid, 0644); err != nil {
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
		e := newEnvironment(cfg, log)
		e.run(c.Context, e.runServer)

		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())

	sigkill := make(chan os.Signal)
	signal.Notify(sigkill, os.Interrupt, os.Kill, syscall.SIGTERM, syscall.SIGQUIT)
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

type task struct {
	Path string `json:"path"`
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

func createAWSSession(cfg *config) *session.Session {
	c := &aws.Config{Region: &cfg.region}
	if cfg.accessKeyID != "" && cfg.secretAccessKey != "" {
		c.Credentials = credentials.NewStaticCredentials(
			cfg.accessKeyID, cfg.secretAccessKey, "")
	}
	return session.Must(session.NewSession(c))
}

func newEnvironment(cfg *config, log *zap.Logger) *environment {
	awsSession := createAWSSession(cfg)
	return &environment{
		config:     *cfg,
		awsSession: awsSession,
		s3Client:   s3.New(awsSession),
		sqsClient:  sqs.New(awsSession),
		log:        log,
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
	entries []*sqs.SendMessageBatchRequestEntry,
	tasks []*task,
) {
	res, err := e.sqsClient.SendMessageBatchWithContext(
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
		if *f.SenderFault {
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
			zap.Bool("sender-fault", *f.SenderFault),
			zap.String("path", tasks[i].Path),
		)
	}
}

func (e *environment) taskSender(ctx context.Context, id string, taskCh <-chan *task, done chan<- struct{}) {
	defer func() {
		done <- struct{}{}
	}()

	entries := make([]*sqs.SendMessageBatchRequestEntry, 0, 10)
	tasks := make([]*task, 0, 10)

	// Never reuse allocated slice since task sender goroutine is still referencing it.
	clearBuf := func() {
		entries = make([]*sqs.SendMessageBatchRequestEntry, 0, 10)
		tasks = make([]*task, 0, 10)
	}

	wg := &sync.WaitGroup{}
	defer wg.Wait()

	idField := zap.String("id", id)

	sendTaskAsync := func(entries []*sqs.SendMessageBatchRequestEntry, tasks []*task) {
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
			jstr := string(jbs)

			entries = append(entries, &sqs.SendMessageBatchRequestEntry{
				Id:          &id,
				MessageBody: &jstr,
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

type efsImageStatus struct {
	time *time.Time
	eTag string
	size int64
	err  error
}

type readSeekCloser interface {
	io.ReadSeeker
	io.Closer
}

type efsImageReader struct {
	reader readSeekCloser
	err    error
}

func (r *efsImageReader) Close() error {
	if r.reader != nil {
		return r.reader.Close()
	}
	return nil
}

func (r *efsImageReader) Seek(offset int64, whence int) (int64, error) {
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
		b.log.Debug("seek called",
			zap.Int64("offset", offset),
			zap.Int("whence", whence),
			zap.Int64("size", b.size))
		return b.Seek(offset, whence)
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
	b.log.Debug("seek called", zap.Int64("offset", offset), zap.Int("whence", whence))
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

type s3ImageStatus struct {
	time *time.Time
	eTag string
	err  error
}

type s3ImageData struct {
	s3ImageStatus
	contentLength int64
	reader        *s3Body
}

func (d *s3ImageData) Close() error {
	if d.reader != nil {
		return d.reader.Close()
	}
	return nil
}

func quoteETag(eTag string) string {
	return "\"" + eTag + "\""
}

// JPNG stands for JPEG/PNG.
func (e *environment) checkJPNGStatus(efsPath string, jpngStatusCh chan<- *efsImageStatus) {
	defer close(jpngStatusCh)

	zapPathField := zap.String("path", efsPath)

	stat, err := os.Stat(efsPath)
	if err != nil {
		if os.IsNotExist(err) {
			e.log.Debug("stat: image file not found", zapPathField)
			jpngStatusCh <- &efsImageStatus{}
			return
		}
		e.log.Error("failed to stat file", zapPathField, zap.Error(err))
		jpngStatusCh <- &efsImageStatus{err: err}
		return
	}

	s := stat.Size()
	t := stat.ModTime().UTC()
	eTag := quoteETag(fmt.Sprintf("%x-%x", t.UnixNano(), s))

	jpngStatusCh <- &efsImageStatus{
		time: &t,
		eTag: eTag,
		size: s,
	}
}

func (e *environment) checkWebPStatus(ctx context.Context, s3Key string, webPStatusCh chan<- *s3ImageStatus) {
	defer close(webPStatusCh)

	s3KeyField := zap.String("key", s3Key)

	res, err := e.s3Client.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: &e.s3Bucket,
		Key:    &s3Key,
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == s3ErrCodeNotFound {
				webPStatusCh <- &s3ImageStatus{}
				return
			}

			e.log.Error("failed to HEAD object",
				s3KeyField,
				zap.String("aws-code", awsErr.Code()),
				zap.String("aws-message", awsErr.Message()))
			webPStatusCh <- &s3ImageStatus{err: err}
			return
		}

		e.log.Error("failed to connect to AWS", s3KeyField, zap.Error(err))
		webPStatusCh <- &s3ImageStatus{err: err}
		return
	}

	if ts := res.Metadata[timestampMetadata]; ts != nil {
		t, err := time.Parse(time.RFC3339Nano, *ts)
		if err != nil {
			e.log.Error("illegal timestamp", s3KeyField, zap.String("timestamp", *ts))
			webPStatusCh <- &s3ImageStatus{err: err}
			return
		}

		webPStatusCh <- &s3ImageStatus{
			time: &t,
			eTag: *res.ETag,
		}
		return
	}

	e.log.Error("timestamp not found", s3KeyField)
	webPStatusCh <- nil
}

func (e *environment) getJPNGReader(ctx context.Context, efsPath string, readerCh chan<- *efsImageReader) {
	defer close(readerCh)

	zapPathField := zap.String("path", efsPath)

	f, err := os.Open(efsPath)
	if err != nil {
		if os.IsNotExist(err) {
			e.log.Debug("open: image file not found", zapPathField)
			readerCh <- &efsImageReader{}
			return
		}
		e.log.Error("failed to open file", zapPathField, zap.Error(err))
		readerCh <- &efsImageReader{err: err}
		return
	}

	readerCh <- &efsImageReader{reader: f}
}

func (e *environment) getWebPReader(ctx context.Context, s3Key string, webPStatusCh chan<- *s3ImageData) {
	defer close(webPStatusCh)

	s3KeyField := zap.String("key", s3Key)

	res, err := e.s3Client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: &e.s3Bucket,
		Key:    &s3Key,
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == s3.ErrCodeNoSuchKey {
				webPStatusCh <- &s3ImageData{}
				return
			}

			e.log.Error("failed to GET object",
				s3KeyField,
				zap.String("aws-code", awsErr.Code()),
				zap.String("aws-message", awsErr.Message()))
			webPStatusCh <- &s3ImageData{
				s3ImageStatus: s3ImageStatus{err: err},
			}
			return
		}

		e.log.Error("failed to connect to AWS", s3KeyField, zap.Error(err))
		webPStatusCh <- &s3ImageData{
			s3ImageStatus: s3ImageStatus{err: err},
		}
		return
	}

	if ts := res.Metadata[timestampMetadata]; ts != nil {
		t, err := time.Parse(time.RFC3339Nano, *ts)
		if err != nil {
			e.log.Error("illegal timestamp", s3KeyField, zap.String("timestamp", *ts))
			webPStatusCh <- &s3ImageData{
				s3ImageStatus: s3ImageStatus{err: err},
			}
			return
		}

		webPStatusCh <- &s3ImageData{
			s3ImageStatus: s3ImageStatus{
				eTag: *res.ETag,
				time: &t,
			},
			contentLength: *res.ContentLength,
			reader: &s3Body{
				body: res.Body,
				size: *res.ContentLength,
				log:  e.log,
			},
		}
		return
	}

	e.log.Error("timestamp not found", s3KeyField)
	webPStatusCh <- nil
}

type filePath struct {
	efs    string
	s3JPNG string
	s3WebP string
	sqs    string
	path   string
	name   string
}

func (e *environment) ensureWebPUpdated(
	ctx context.Context,
	jpngStatus *efsImageStatus,
	webPStatus *s3ImageStatus,
	jpngReader *efsImageReader,
	fpath *filePath,
	taskCh chan<- *task,
) {
	zapPathField := zap.String("path", fpath.efs)

	//
	// The principle is that never change file if error occurs.
	//

	if jpngStatus.err != nil || webPStatus.err != nil {
		return
	}

	// Do nothing since the both files don't exist.
	if jpngStatus.time == nil && webPStatus.time == nil {
		return
	}

	if jpngStatus.time != nil && webPStatus.time != nil && jpngStatus.time.Equal(*webPStatus.time) {
		return
	}

	if jpngReader == nil {
		jpngReaderCh := make(chan *efsImageReader)
		go e.getJPNGReader(ctx, fpath.efs, jpngReaderCh)
		jpngReader = <-jpngReaderCh
		defer func() {
			if err := jpngReader.Close(); err != nil {
				e.log.Error("failed to close EFS file", zapPathField, zap.Error(err))
			}
		}()
	} else {
		jpngReader.Seek(0, io.SeekStart)
	}

	if jpngReader.err != nil {
		return
	}

	if jpngReader.reader != nil && jpngStatus.time != nil {
		// PUT the latest image file for processing.
		var contentType string
		switch filepath.Ext(fpath.s3JPNG) {
		case ".jpg":
			contentType = jpegContentType
		case ".png":
			contentType = pngContentType
		default:
			e.log.Error("unknown extension for images", zapPathField)
			return
		}

		tsStr := jpngStatus.time.Format(time.RFC3339Nano)
		if _, err := e.s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
			Bucket:      &e.s3Bucket,
			Key:         &fpath.s3JPNG,
			Body:        jpngReader.reader,
			ContentType: &contentType,
			Metadata: map[string]*string{
				pathMetadata:      &fpath.path,
				timestampMetadata: &tsStr,
			},
		}); err != nil {
			e.log.Error("failed to PUT S3 source image",
				zapPathField, zap.Error(err), zap.String("s3key", fpath.s3JPNG))
			return
		}
	}
	// Do nothing and just add task when the file was deleted from EFS.
	// The task will delete the corresponding WebP file when no source image exists.

	select {
	case taskCh <- &task{Path: fpath.sqs}:
	case <-ctx.Done():
	}
}

func (e *environment) respondWithEFSFile(
	c *gin.Context,
	fpath *filePath,
	status *efsImageStatus,
	reader *efsImageReader,
	cache string,
) {
	if reader.err != nil {
		c.String(http.StatusInternalServerError, "failed to handle request")
		return
	}
	body := &efsFileBody{
		body: reader.reader,
		size: status.size,
		log:  e.log,
	}

	c.Writer.Header().Set(cacheControlHeader, cache)
	c.Writer.Header().Set(eTagHeader, status.eTag)
	http.ServeContent(c.Writer, c.Request, fpath.name, *status.time, body)
	c.Writer.Flush()
}

func (e *environment) handleJPNGRequest(c *gin.Context, fpath *filePath, taskCh chan<- *task) {
	jpngReaderCh := make(chan *efsImageReader)
	jpngStatusCh := make(chan *efsImageStatus)
	webPStatusCh := make(chan *s3ImageStatus)

	go e.checkJPNGStatus(fpath.efs, jpngStatusCh)
	go e.getJPNGReader(c, fpath.efs, jpngReaderCh)
	go e.checkWebPStatus(c, fpath.s3WebP, webPStatusCh)

	jpngStatus := <-jpngStatusCh
	jpngReader := <-jpngReaderCh

	defer func() {
		if err := jpngReader.Close(); err != nil {
			e.log.Error("failed to close EFS file", zap.Error(err), zap.String("path", fpath.efs))
		}
	}()

	if jpngReader.err != nil || jpngStatus.err != nil {
		e.respondWithInternalServerErrorText(c)
	} else if jpngReader.reader == nil || jpngStatus.time == nil {
		e.respondWithNotFoundText(c)
	} else {
		e.respondWithEFSFile(c, fpath, jpngStatus, jpngReader, e.permanentCache)
	}

	// Now ensure WebP file exists.
	webPStatus := <-webPStatusCh

	e.ensureWebPUpdated(c, jpngStatus, webPStatus, jpngReader, fpath, taskCh)
}

func (e *environment) respondWithInternalServerErrorText(c *gin.Context) {
	const message = "internal server error"
	c.Writer.Header().Set(cacheControlHeader, e.temporaryCache)
	c.Writer.Header().Set(contentLengthHeader, strconv.Itoa(len(message)))
	c.String(http.StatusInternalServerError, message)
	c.Writer.Flush()
}

func (e *environment) respondWithNotFoundText(c *gin.Context) {
	const message = "file not found"
	c.Writer.Header().Set(cacheControlHeader, e.temporaryCache)
	c.Writer.Header().Set(contentLengthHeader, strconv.Itoa(len(message)))
	c.String(http.StatusNotFound, message)
	c.Writer.Flush()
}

func (e *environment) respondWithS3Object(c *gin.Context, fpath *filePath, webPData *s3ImageData) {
	defer func() {
		if err := webPData.Close(); err != nil {
			e.log.Error("failed to close S3 body", zap.Error(err), zap.String("path", fpath.s3WebP))
		}
	}()

	c.Writer.Header().Set(cacheControlHeader, e.permanentCache)
	c.Writer.Header().Set(eTagHeader, webPData.eTag)
	c.Writer.Header().Set(contentTypeHeader, webPContentType)
	http.ServeContent(c.Writer, c.Request, "", *webPData.time, webPData.reader)
	c.Writer.Flush()
}

func (e *environment) respondTemporarily(
	c *gin.Context,
	fpath *filePath,
	jpngStatus *efsImageStatus,
	jpngReader *efsImageReader,
) {
	if jpngReader.err != nil || jpngStatus.err != nil {
		e.respondWithInternalServerErrorText(c)
	} else if jpngReader.reader == nil || jpngStatus.time == nil {
		e.respondWithNotFoundText(c)
	} else {
		e.respondWithEFSFile(c, fpath, jpngStatus, jpngReader, e.temporaryCache)
	}
}

func (e *environment) handleWebPRequest(c *gin.Context, fpath *filePath, taskCh chan<- *task) {
	webPDataCh := make(chan *s3ImageData)
	jpngStatusCh := make(chan *efsImageStatus)

	go e.getWebPReader(c, fpath.s3WebP, webPDataCh)
	go e.checkJPNGStatus(fpath.efs, jpngStatusCh)

	webPData := <-webPDataCh
	var jpngStatus *efsImageStatus
	var jpngReader *efsImageReader
	if webPData.err != nil {
		e.log.Info(
			"WebP is requested but JPEG/PNG will be responded due to error on S3",
			zap.String("path", fpath.efs),
			zap.Error(webPData.err))
		jpngReaderCh := make(chan *efsImageReader)
		go e.getJPNGReader(c, fpath.efs, jpngReaderCh)
		jpngStatus = <-jpngStatusCh

		jpngReader = <-jpngReaderCh
		defer func() {
			if err := jpngReader.Close(); err != nil {
				e.log.Error("failed to close EFS file", zap.Error(err), zap.String("path", fpath.efs))
			}
		}()

		e.respondTemporarily(c, fpath, jpngStatus, jpngReader)
	} else if webPData.time != nil {
		e.respondWithS3Object(c, fpath, webPData)
		jpngStatus = <-jpngStatusCh
	} else {
		jpngReaderCh := make(chan *efsImageReader)
		go e.getJPNGReader(c, fpath.efs, jpngReaderCh)
		// WebP not yet generated
		jpngStatus = <-jpngStatusCh

		jpngReader = <-jpngReaderCh
		defer func() {
			if err := jpngReader.Close(); err != nil {
				e.log.Error("failed to close EFS file", zap.Error(err), zap.String("path", fpath.efs))
			}
		}()

		e.respondTemporarily(c, fpath, jpngStatus, jpngReader)
	}

	e.ensureWebPUpdated(c, jpngStatus, &webPData.s3ImageStatus, jpngReader, fpath, taskCh)
}

func (e *environment) handleRequest(c *gin.Context, path string, acceptHeader string, taskCh chan<- *task) {
	// path value contains leading "/".

	// Sanitize and reject malicious path
	efsAbsPath := filepath.Clean(filepath.Join(e.efsMountPath, path))
	if !strings.HasPrefix(efsAbsPath, e.efsMountPath) {
		c.String(http.StatusBadRequest, "invalid URL path")
		return
	}
	_, name := filepath.Split(path)
	sanitizedPath := strings.TrimPrefix(efsAbsPath, e.efsMountPath+"/")

	fpath := &filePath{
		efs:    efsAbsPath,
		s3JPNG: filepath.Clean(filepath.Join(e.s3SrcKeyBase, path)),
		s3WebP: filepath.Clean(filepath.Join(e.s3DestKeyBase, path+".webp")),
		sqs:    sanitizedPath,
		path:   sanitizedPath,
		name:   name,
	}

	if supportsWebP(acceptHeader) {
		e.handleWebPRequest(c, fpath, taskCh)
	} else {
		e.handleJPNGRequest(c, fpath, taskCh)
	}
}
