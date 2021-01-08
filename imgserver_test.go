package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

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

func (s *ImgServerSuite) uploadToS3(
	ctx context.Context,
	path string,
	lastModified *time.Time,
) string {
	contentType := webPContentType
	key := s.env.s3KeyBase + "/" + path + ".webp"
	f, err := os.Open(sampleJPEGWebP)
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
			"Original-Path":      &path,
			"Original-Timestamp": &tsStr,
		},
	})
	s.Require().NoError(err)

	return quoteETag(*res.ETag)
}

func (s *ImgServerSuite) Test_Accepted_S3_EFS() {
	const path = "dir/image000.jpg"
	eTag := s.uploadToS3(s.ctx, path, nil)

	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, chromeAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusOK, res.StatusCode)
		s.Assert().Equal(s.env.config.permanentCache, header.cacheControl())
		s.Assert().Equal(webPContentType, header.contentType())
		s.Assert().Equal(sampleJPEGWebPSize, res.ContentLength)
		s.Assert().Equal(eTag, header.eTag())
		s.Assert().Equal(sampleLastModified, header.lastModified())

		s.assertNoSQSMessage(ctx)
	})
}

func (s *ImgServerSuite) Test_Accepted_S3_NoEFS() {
	const path = "dir/image000.jpg"
	eTag := s.uploadToS3(s.ctx, path, nil)
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

		s.assertDelayedSQSMessage(ctx, path)
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

		s.assertDelayedSQSMessage(ctx, path)
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

		s.assertNoSQSMessage(ctx)
	})
}

func (s *ImgServerSuite) Test_Unaccepted_S3_EFS() {
	const path = "dir/image000.jpg"
	s.uploadToS3(s.ctx, path, nil)
	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, oldSafariAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusOK, res.StatusCode)
		s.Assert().Equal(s.env.config.permanentCache, header.cacheControl())
		s.Assert().Equal(jpegContentType, header.contentType())
		s.Assert().Equal(sampleJPEGSize, res.ContentLength)
		s.Assert().Equal(sampleJPEGETag, header.eTag())
		s.Assert().Equal(sampleLastModified, header.lastModified())

		s.assertNoSQSMessage(ctx)
	})
}

func (s *ImgServerSuite) Test_Unaccepted_S3_NoEFS() {
	const (
		path        = "dir/image000.jpg"
		longTextLen = int64(1024)
	)

	s.uploadToS3(s.ctx, path, nil)
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

		s.assertDelayedSQSMessage(ctx, path)
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

		s.assertDelayedSQSMessage(ctx, path)
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

		s.assertNoSQSMessage(ctx)
	})
}

func (s *ImgServerSuite) Test_Accepted_S3_EFS_Old() {
	const path = "dir/image000.jpg"
	eTag := s.uploadToS3(s.ctx, path, &oldModTime)

	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, chromeAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusOK, res.StatusCode)
		s.Assert().Equal(s.env.config.permanentCache, header.cacheControl())
		s.Assert().Equal(webPContentType, header.contentType())
		s.Assert().Equal(sampleJPEGWebPSize, res.ContentLength)
		s.Assert().Equal(eTag, header.eTag())
		s.Assert().Equal(oldLastModified, header.lastModified())

		// Send message to update S3 object
		s.assertDelayedSQSMessage(ctx, path)
	})
}

func (s *ImgServerSuite) Test_Accepted_NoS3_EFS_BatchSendRepeat() {
	s.serve(func(ctx context.Context, ts *httptest.Server) {
		for i := 0; i < 30; i++ {
			s.request(ctx, ts, fmt.Sprintf("/dir/image%03d.jpg", i), chromeAcceptHeader)
		}
		time.Sleep(time.Second)

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
