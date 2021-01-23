package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/suite"
)

type ImgServerSuite struct {
	*TestSuite
}

func TestImgServerSuite(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	s := &ImgServerSuite{TestSuite: initTestSuite("imgserver", t)}
	suite.Run(t, s)
}

func (s *ImgServerSuite) SetupTest() {
	s.env = newTestEnvironment("imgserver", s.TestSuite)

	s.Require().NoError(os.MkdirAll(s.env.efsMountPath+"/dir", 0755))

	for i := 0; i < 50; i++ {
		copy(sampleJPEG, fmt.Sprintf("%s/dir/image%03d.jpg", s.env.efsMountPath, i), &s.Suite)
		copy(samplePNG, fmt.Sprintf("%s/dir/image%03d.png", s.env.efsMountPath, i), &s.Suite)
	}
}

func (s *ImgServerSuite) TearDownTest() {
	cleanTestEnvironment(s.ctx, s.TestSuite)
}

func (s *ImgServerSuite) serve(do func(context.Context, *httptest.Server)) {
	s.env.run(s.ctx, func(ctx context.Context, engine *gin.Engine) {
		ts := httptest.NewServer(engine)
		defer ts.Close()

		do(ctx, ts)
	})
}

func (s *ImgServerSuite) receiveSQSMessages(ctx context.Context) []*sqs.Message {
	visibilityTimeout := int64(5)
	maxNumberOfMessages := int64(10)

	messages := []*sqs.Message{}

	for {
		res, err := s.env.sqsClient.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            &s.env.sqsQueueURL,
			VisibilityTimeout:   &visibilityTimeout,
			MaxNumberOfMessages: &maxNumberOfMessages,
		})
		s.Require().NoError(err)
		if len(res.Messages) == 0 {
			break
		}

		messages = append(messages, res.Messages...)
	}

	return messages
}

func (s *ImgServerSuite) deleteSQSMessages(ctx context.Context, messages []*sqs.Message) {
	entries := make([]*sqs.DeleteMessageBatchRequestEntry, 0, 10)
	for i, msg := range messages {
		id := strconv.Itoa(i)
		entries = append(entries, &sqs.DeleteMessageBatchRequestEntry{
			Id:            &id,
			ReceiptHandle: msg.ReceiptHandle,
		})
	}

	res, err := s.env.sqsClient.DeleteMessageBatchWithContext(
		ctx,
		&sqs.DeleteMessageBatchInput{
			QueueUrl: &s.env.sqsQueueURL,
			Entries:  entries,
		})
	s.Require().NoError(err)
	s.Require().Empty(res.Failed)
}

func (s *ImgServerSuite) request(
	ctx context.Context,
	ts *httptest.Server,
	path string,
	accept string,
) *http.Response {
	req, err := http.NewRequest(http.MethodGet, ts.URL+path, nil)
	s.Require().NoError(err)
	req.Header.Set(acceptHeader, accept)
	res, err := http.DefaultClient.Do(req.WithContext(ctx))
	s.Require().NoError(err)
	return res
}

func (s *ImgServerSuite) assertDelayedSQSMessage(ctx context.Context, path string) {
	time.Sleep(time.Second)

	s.Assert().Empty(s.receiveSQSMessages(ctx))

	time.Sleep(2 * time.Second)

	msgs := s.receiveSQSMessages(ctx)
	s.Assert().Len(msgs, 1)
	t := &task{}
	s.Assert().NoError(json.Unmarshal([]byte(*msgs[0].Body), t))
	s.Assert().Equal(path, t.Path)
}

func (s *ImgServerSuite) assertNoSQSMessage(ctx context.Context) {
	time.Sleep(3 * time.Second)

	msgs := s.receiveSQSMessages(ctx)
	s.Assert().Empty(msgs)
}

func (s *ImgServerSuite) assertS3SrcExists(
	ctx context.Context,
	path string,
	lastModified *time.Time,
	contentType string,
	contentLength int64,
) {
	key := s.env.s3SrcKeyBase + "/" + path
	res, err := s.env.s3Client.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: &s.env.s3Bucket,
		Key:    &key,
	})
	s.Assert().NoError(err)
	s.Assert().Equal(path, *res.Metadata[pathMetadata])
	t, err := time.Parse(time.RFC3339Nano, *res.Metadata[timestampMetadata])
	s.Assert().NoError(err)
	s.Assert().Equal(lastModified.UTC(), t)
	s.Assert().Equal(contentType, *res.ContentType)
	s.Assert().Equal(contentLength, *res.ContentLength)
}

func (s *ImgServerSuite) assertS3SrcNotExists(ctx context.Context, path string) {
	key := s.env.s3SrcKeyBase + "/" + path
	_, err := s.env.s3Client.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: &s.env.s3Bucket,
		Key:    &key,
	})
	s.Assert().NotNil(err)
	awsErr, ok := err.(awserr.Error)
	s.Assert().True(ok)
	s.Assert().Equal(s3ErrCodeNotFound, awsErr.Code())
}

func (s *ImgServerSuite) uploadToS3(
	ctx context.Context,
	key string,
	bodyPath string,
	contentType string,
	metadataPath string,
	lastModified *time.Time,
) string {
	f, err := os.Open(bodyPath)
	s.Require().NoError(err)
	defer func() {
		s.Require().NoError(f.Close())
	}()

	var ts *time.Time
	if lastModified != nil {
		ts = lastModified
	} else {
		ts = &sampleModTime
	}
	tsStr := ts.Format(time.RFC3339Nano)

	res, err := s.env.s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket:      &s.env.s3Bucket,
		Key:         &key,
		Body:        f,
		ContentType: &contentType,
		Metadata: map[string]*string{
			pathMetadata:      &metadataPath,
			timestampMetadata: &tsStr,
		},
	})
	s.Require().NoError(err)

	return *res.ETag
}

func (s *ImgServerSuite) uploadWebPToS3(
	ctx context.Context,
	path string,
	bodyPath string,
	lastModified *time.Time,
) string {
	return s.uploadToS3(
		ctx,
		s.env.s3DestKeyBase+"/"+path+".webp",
		bodyPath,
		webPContentType,
		path,
		lastModified)
}

func (s *ImgServerSuite) uploadJPNGToS3(
	ctx context.Context,
	path string,
	bodyPath string,
	lastModified *time.Time,
) string {
	var contentType string
	switch filepath.Ext(path) {
	case ".jpg":
		contentType = jpegContentType
	case ".png":
		contentType = pngContentType
	default:
		s.Require().Fail("unknown image type")
	}

	return s.uploadToS3(
		ctx,
		s.env.s3DestKeyBase+"/"+path,
		bodyPath,
		contentType,
		path,
		lastModified)
}

func (s *ImgServerSuite) Test_Accepted_S3_EFS() {
	const path = "dir/image000.jpg"
	eTag := s.uploadWebPToS3(s.ctx, path, sampleJPEGWebP, nil)

	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, chromeAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusOK, res.StatusCode)
		s.Assert().Equal(s.env.config.permanentCache, header.cacheControl())
		s.Assert().Equal(webPContentType, header.contentType())
		s.Assert().Equal(sampleJPEGWebPSize, res.ContentLength)
		s.Assert().Equal(eTag, header.eTag())
		s.Assert().Equal(sampleLastModified, header.lastModified())
		body, err := ioutil.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		s.assertNoSQSMessage(ctx)
		s.assertS3SrcNotExists(ctx, path)
	})
}

func (s *ImgServerSuite) Test_Accepted_S3_NoEFS() {
	const path = "dir/image000.jpg"
	eTag := s.uploadWebPToS3(s.ctx, path, sampleJPEGWebP, nil)
	s.Require().NoError(os.Remove(s.env.efsMountPath + "/" + path))

	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, chromeAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusOK, res.StatusCode)
		s.Assert().Equal(s.env.config.permanentCache, header.cacheControl())
		s.Assert().Equal(webPContentType, header.contentType())
		s.Assert().Equal(sampleJPEGWebPSize, res.ContentLength)
		s.Assert().Equal(eTag, header.eTag())
		s.Assert().Equal(sampleLastModified, header.lastModified())
		body, err := ioutil.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		s.assertDelayedSQSMessage(ctx, path)
		s.assertS3SrcNotExists(ctx, path)
	})
}

func (s *ImgServerSuite) Test_Accepted_NoS3_EFS() {
	const path = "dir/image000.jpg"
	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, chromeAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusOK, res.StatusCode)
		s.Assert().Equal(s.env.config.temporaryCache, header.cacheControl())
		s.Assert().Equal(jpegContentType, header.contentType())
		s.Assert().Equal(sampleJPEGSize, res.ContentLength)
		s.Assert().Equal(sampleJPEGETag, header.eTag())
		s.Assert().Equal(sampleLastModified, header.lastModified())
		body, err := ioutil.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		s.assertDelayedSQSMessage(ctx, path)
		s.assertS3SrcExists(ctx, path, &sampleModTime, jpegContentType, sampleJPEGSize)
	})
}

func (s *ImgServerSuite) Test_Accepted_NoS3_NoEFS() {
	const (
		path        = "dir/nonexistent.jpg"
		longTextLen = int64(1024)
	)

	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, chromeAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusNotFound, res.StatusCode)
		s.Assert().Equal(s.env.config.temporaryCache, header.cacheControl())
		s.Assert().Equal(plainContentType, header.contentType())
		s.Assert().Greater(longTextLen, res.ContentLength)
		s.Assert().Equal("", header.eTag())
		s.Assert().Equal("", header.lastModified())
		body, err := ioutil.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		s.assertNoSQSMessage(ctx)
		s.assertS3SrcNotExists(ctx, path)
	})
}

func (s *ImgServerSuite) Test_Unaccepted_S3_EFS() {
	const path = "dir/image000.jpg"
	s.uploadWebPToS3(s.ctx, path, sampleJPEGWebP, nil)
	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, oldSafariAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusOK, res.StatusCode)
		s.Assert().Equal(s.env.config.permanentCache, header.cacheControl())
		s.Assert().Equal(jpegContentType, header.contentType())
		s.Assert().Equal(sampleJPEGSize, res.ContentLength)
		s.Assert().Equal(sampleJPEGETag, header.eTag())
		s.Assert().Equal(sampleLastModified, header.lastModified())
		body, err := ioutil.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		s.assertNoSQSMessage(ctx)
		s.assertS3SrcNotExists(ctx, path)
	})
}

func (s *ImgServerSuite) Test_Unaccepted_S3_NoEFS() {
	const (
		path        = "dir/image000.jpg"
		longTextLen = int64(1024)
	)

	s.uploadWebPToS3(s.ctx, path, sampleJPEGWebP, nil)
	s.Require().NoError(os.Remove(s.env.efsMountPath + "/" + path))
	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, oldSafariAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusNotFound, res.StatusCode)
		s.Assert().Equal(s.env.config.temporaryCache, header.cacheControl())
		s.Assert().Equal(plainContentType, header.contentType())
		s.Assert().Greater(longTextLen, res.ContentLength)
		s.Assert().Equal("", header.eTag())
		s.Assert().Equal("", header.lastModified())
		body, err := ioutil.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		s.assertDelayedSQSMessage(ctx, path)
		s.assertS3SrcNotExists(ctx, path)
	})
}

func (s *ImgServerSuite) Test_Unaccepted_NoS3_EFS() {
	const path = "dir/image000.jpg"
	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, oldSafariAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusOK, res.StatusCode)
		s.Assert().Equal(s.env.config.permanentCache, header.cacheControl())
		s.Assert().Equal(jpegContentType, header.contentType())
		s.Assert().Equal(sampleJPEGSize, res.ContentLength)
		s.Assert().Equal(sampleJPEGETag, header.eTag())
		s.Assert().Equal(sampleLastModified, header.lastModified())
		body, err := ioutil.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		s.assertDelayedSQSMessage(ctx, path)
		s.assertS3SrcExists(ctx, path, &sampleModTime, jpegContentType, sampleJPEGSize)
	})
}

func (s *ImgServerSuite) Test_Unaccepted_NoS3_NoEFS() {
	const (
		path        = "dir/nonexistent.jpg"
		longTextLen = int64(1024)
	)

	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, oldSafariAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusNotFound, res.StatusCode)
		s.Assert().Equal(s.env.config.temporaryCache, header.cacheControl())
		s.Assert().Equal(plainContentType, header.contentType())
		s.Assert().Greater(longTextLen, res.ContentLength)
		s.Assert().Equal("", header.eTag())
		s.Assert().Equal("", header.lastModified())
		body, err := ioutil.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		s.assertNoSQSMessage(ctx)
		s.assertS3SrcNotExists(ctx, path)
	})
}

func (s *ImgServerSuite) Test_Accepted_S3_EFS_Old() {
	const path = "dir/image000.jpg"
	eTag := s.uploadWebPToS3(s.ctx, path, sampleJPEGWebP, &oldModTime)

	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, chromeAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusOK, res.StatusCode)
		s.Assert().Equal(s.env.config.permanentCache, header.cacheControl())
		s.Assert().Equal(webPContentType, header.contentType())
		s.Assert().Equal(sampleJPEGWebPSize, res.ContentLength)
		s.Assert().Equal(eTag, header.eTag())
		s.Assert().Equal(oldLastModified, header.lastModified())
		body, err := ioutil.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		// Send message to update S3 object
		s.assertDelayedSQSMessage(ctx, path)
		s.assertS3SrcExists(ctx, path, &sampleModTime, jpegContentType, sampleJPEGSize)
	})
}

func (s *ImgServerSuite) Test_Accepted_NoS3_EFS_BatchSendRepeat() {
	s.serve(func(ctx context.Context, ts *httptest.Server) {
		for i := 0; i < 30; i++ {
			s.request(ctx, ts, fmt.Sprintf("/dir/image%03d.jpg", i), chromeAcceptHeader)
		}
		time.Sleep(3 * time.Second)

		msgs := s.receiveSQSMessages(ctx)
		s.Assert().Len(msgs, 30)
	})
}

func (s *ImgServerSuite) Test_Accepted_NoS3_EFS_BatchSendWait() {
	s.env.config.sqsBatchWaitTime = 5

	s.serve(func(ctx context.Context, ts *httptest.Server) {
		for i := 0; i < 15; i++ {
			s.request(ctx, ts, fmt.Sprintf("/dir/image%03d.jpg", i), chromeAcceptHeader)
		}
		time.Sleep(3 * time.Second)

		msgs := s.receiveSQSMessages(ctx)
		s.Assert().Len(msgs, 10)
		s.deleteSQSMessages(ctx, msgs)

		s.Assert().Empty(s.receiveSQSMessages(ctx))

		time.Sleep(3 * time.Second)

		s.Assert().Len(s.receiveSQSMessages(ctx), 5)
	})
}

func (s *ImgServerSuite) Test_ReopenLogFile() {
	oldLogPath := s.env.efsMountPath + "/imgserver.log.old"
	currentLogPath := s.env.efsMountPath + "/imgserver.log"

	s.env.log.Info("first message")

	s.Require().NoError(os.Rename(currentLogPath, oldLogPath))

	s.env.log.Info("second message")

	p, err := os.FindProcess(os.Getpid())
	s.Require().NoError(err)
	s.Require().NoError(p.Signal(syscall.SIGUSR1)) // Reopen log files

	time.Sleep(time.Second)

	s.env.log.Info("third message")

	oldBytes, err := ioutil.ReadFile(oldLogPath)
	s.Require().NoError(err)

	currentBytes, err := ioutil.ReadFile(currentLogPath)
	s.Require().NoError(err)

	oldLog := string(oldBytes)
	currentLog := string(currentBytes)

	s.Assert().Contains(oldLog, "first")
	s.Assert().Contains(oldLog, "second")

	s.Assert().Contains(currentLog, "third")
}
