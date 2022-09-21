package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"runtime"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
)

const (
	sampleJPEG       = "samplefile/image.jpg"
	sampleJPEGWebP   = "samplefile/image.jpg.webp"
	samplePNG        = "samplefile/image.png"
	samplePNGWebP    = "samplefile/image.png.webp"
	sampleJS         = "samplefile/script.js"
	sampleMinJS      = "samplefile/script.min.js"
	sampleSourceMap  = "samplefile/script.js.map"
	sampleSourceMap2 = "samplefile/script2.js.map"
	sampleCSS        = "samplefile/style.css"
	sampleMinCSS     = "samplefile/style.min.css"

	sampleJPEGSize       = int64(23838)
	sampleJPEGWebPSize   = int64(5294)
	samplePNGSize        = int64(28877)
	samplePNGWebPSize    = int64(5138)
	sampleJSSize         = int64(335)
	sampleMinJSSize      = int64(285)
	sampleSourceMapSize  = int64(723)
	sampleSourceMap2Size = int64(276)
	sampleCSSSize        = int64(91)
	sampleMinCSSSize     = int64(58)

	chromeAcceptHeader    = "image/avif,image/webp,image/apng,image/*,*/*;q=0.8"
	oldSafariAcceptHeader = "image/png,image/svg+xml,image/*;q=0.8,video/*;q=0.8,*/*;q=0.5"

	publicContentPathPattern = "wp-content/uploads/*"
)

func sampleETag(size int64) string {
	return fmt.Sprintf("\"%x-%x\"", sampleModTime.UnixNano(), size)
}

var (
	oldModTime           = time.Date(2019, time.January, 1, 0, 0, 0, 0, time.UTC)
	sampleModTime        = time.Date(2021, time.January, 1, 0, 0, 0, 0, time.UTC)
	sampleLastModified   = sampleModTime.Format(http.TimeFormat)
	sampleJPEGETag       = sampleETag(sampleJPEGSize)
	sampleJSETag         = sampleETag(sampleJSSize)
	sampleMinJSETag      = sampleETag(sampleMinJSSize)
	sampleCSSETag        = sampleETag(sampleCSSSize)
	sampleMinCSSETag     = sampleETag(sampleMinCSSSize)
	sampleSourceMap2ETag = sampleETag(sampleSourceMap2Size)
)

// InitTest moves working directory to project root directory.
// https://brandur.org/fragments/testing-go-project-root
func InitTest() {
	_, filename, _, _ := runtime.Caller(0)
	dir := path.Join(path.Dir(filename), ".")
	err := os.Chdir(dir)
	if err != nil {
		panic(err)
	}
}

func readTestConfig(name string) string {
	cwd, err := os.Getwd()
	if err != nil {
		panic("failed to get current working directory")
	}

	path := cwd + "/config/test/" + name
	val, err := ioutil.ReadFile(path)
	if err != nil {
		panic("failed to load config file: " + path + ", error: " + err.Error())
	}
	return strings.TrimSpace(string(val))
}

func generateSafeRandomString() string {
	v := make([]byte, 256/8)
	if _, err := rand.Read(v); err != nil {
		panic(err.Error())
	}

	return base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(v)
}

func getTestConfig(name string) *configure {
	region := "ap-northeast-1"
	sqsName := "test-" + name + "-" + generateSafeRandomString()
	sqsURL := fmt.Sprintf("https://sqs.%s.amazonaws.com/%s/%s",
		region,
		readTestConfig("aws-account-id"),
		sqsName)
	efsPath := fmt.Sprintf("work/test/%s/%s", name, generateSafeRandomString())
	publicCotnentPathPatterns := "foobarbaz/*,/" + generateSafeRandomString() + "/" + publicContentPathPattern

	cfg := &configure{
		region:                    region,
		accessKeyID:               readTestConfig("access-key-id"),
		secretAccessKey:           readTestConfig("secret-access-key"),
		s3Bucket:                  readTestConfig("s3-bucket"),
		s3SrcPrefix:               generateSafeRandomString() + "/" + name + "/",
		s3DestPrefix:              generateSafeRandomString() + "/" + name + "/",
		sqsQueueURL:               sqsURL,
		sqsBatchWaitTime:          2,
		efsMountPath:              efsPath,
		gracefulShutdownTimeout:   5,
		port:                      0, // Not used
		logPath:                   efsPath + "/imgserver.log",
		errorLogPath:              efsPath + "/imgserver-error.log",
		publicCotnentS3Bucket:     readTestConfig("public-content-s3-bucket"),
		publicCotnentPathPatterns: publicCotnentPathPatterns,
		publicCotnentPathGlob:     createCloudfrontPathGlob(publicCotnentPathPatterns),
		temporaryCache: &cacheControl{
			name:  "temporary",
			value: fmt.Sprintf("public, max-age=%d", 20*60),
		},
		permanentCache: &cacheControl{
			name:  "permanent",
			value: fmt.Sprintf("public, max-age=%d", 365*24*60*60),
		},
	}

	return cfg
}

func getTestSQSQueueNameFromURL(url string) string {
	parts := strings.Split(url, "/")
	return parts[len(parts)-1]
}

func newTestEnvironment(name string, s *TestSuite) *environment {
	cfg := getTestConfig(name)

	require.NoError(
		s.T(),
		os.RemoveAll(cfg.efsMountPath),
		"failed to remove directory")

	require.NoError(
		s.T(),
		os.MkdirAll(cfg.efsMountPath, 0755),
		"failed to create directory")

	log := createLogger(s.ctx, cfg.logPath, cfg.errorLogPath)
	e := newEnvironment(s.ctx, cfg, log)

	sqsName := getTestSQSQueueNameFromURL(e.sqsQueueURL)
	_, err := e.sqsClient.CreateQueue(s.ctx, &sqs.CreateQueueInput{
		QueueName: &sqsName,
	})
	require.NoError(s.T(), err, "failed to create SQS queue")

	return e
}

func initTestSuite(name string, t require.TestingT) *TestSuite {
	InitTest()
	require.NoError(t, os.RemoveAll("work/test/"+name), "failed to remove directory")
	ctx := context.Background()

	return &TestSuite{ctx: ctx}
}

func cleanTestEnvironment(ctx context.Context, s *TestSuite) {
	if _, err := s.env.sqsClient.DeleteQueue(ctx, &sqs.DeleteQueueInput{
		QueueUrl: &s.env.sqsQueueURL,
	}); err != nil {
		s.env.log.Error("failed to clean up SQS queue", zap.Error(err))
	}
}

// TestSuite holds configs and sessions required to execute program.
type TestSuite struct {
	suite.Suite
	env *environment
	ctx context.Context
}

func copy(src, dst string, s *suite.Suite) {
	in, err := os.Open(src)
	s.Require().NoError(err)
	defer func() {
		s.Require().NoError(in.Close())
	}()

	out, err := os.Create(dst)
	s.Require().NoError(err)
	defer func() {
		s.Require().NoError(out.Close())
		s.Require().NoError(os.Chtimes(dst, sampleModTime, sampleModTime))
	}()

	{
		_, err := io.Copy(out, in)
		s.Require().NoError(err)
	}
}

type httpHeader http.Response

func (h *httpHeader) cacheControl() string {
	return h.Header.Get(cacheControlHeader)
}

func (h *httpHeader) contentType() string {
	return h.Header.Get(contentTypeHeader)
}

func (h *httpHeader) eTag() string {
	return h.Header.Get(eTagHeader)
}

func (h *httpHeader) lastModified() string {
	return h.Header.Get(lastModifiedHeader)
}
