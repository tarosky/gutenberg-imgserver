package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqst "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
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

	for i := 0; i < 25; i++ {
		copy(sampleJPEG, fmt.Sprintf("%s/dir/image%03d.jpg", s.env.efsMountPath, i), &s.Suite)
		copy(samplePNG, fmt.Sprintf("%s/dir/image%03d.png", s.env.efsMountPath, i), &s.Suite)
	}
	for i := 25; i < 50; i++ {
		copy(sampleJPEG, fmt.Sprintf("%s/dir/image%03d.JPG", s.env.efsMountPath, i), &s.Suite)
		copy(samplePNG, fmt.Sprintf("%s/dir/image%03d.PNG", s.env.efsMountPath, i), &s.Suite)
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

func (s *ImgServerSuite) receiveSQSMessages(ctx context.Context) []sqst.Message {
	messages := []sqst.Message{}

	for {
		res, err := s.env.sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            &s.env.sqsQueueURL,
			VisibilityTimeout:   5,
			MaxNumberOfMessages: 10,
		})
		s.Require().NoError(err)
		if len(res.Messages) == 0 {
			break
		}

		messages = append(messages, res.Messages...)
	}

	return messages
}

func (s *ImgServerSuite) deleteSQSMessages(ctx context.Context, messages []sqst.Message) {
	entries := make([]sqst.DeleteMessageBatchRequestEntry, 0, 10)
	for i, msg := range messages {
		id := strconv.Itoa(i)
		entries = append(entries, sqst.DeleteMessageBatchRequestEntry{
			Id:            &id,
			ReceiptHandle: msg.ReceiptHandle,
		})
	}

	res, err := s.env.sqsClient.DeleteMessageBatch(
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
	res, err := s.env.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &s.env.s3Bucket,
		Key:    aws.String(s.env.s3SrcKeyBase + "/" + path),
	})
	s.Assert().NoError(err)
	s.Assert().Equal(path, res.Metadata[pathMetadata])
	t, err := time.Parse(time.RFC3339Nano, res.Metadata[timestampMetadata])
	s.Assert().NoError(err)
	s.Assert().Equal(lastModified.UTC(), t)
	s.Assert().Equal(contentType, *res.ContentType)
	s.Assert().Equal(contentLength, res.ContentLength)
}

func (s *ImgServerSuite) assertS3SrcNotExists(ctx context.Context, path string) {
	_, err := s.env.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &s.env.s3Bucket,
		Key:    aws.String(s.env.s3SrcKeyBase + "/" + path),
	})
	s.Assert().NotNil(err)
	var apiErr smithy.APIError
	errors.As(err, &apiErr)
	s.Assert().Equal(s3ErrCodeNotFound, apiErr.ErrorCode())
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

	res, err := s.env.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &s.env.s3Bucket,
		Key:         &key,
		Body:        f,
		ContentType: &contentType,
		Metadata: map[string]string{
			pathMetadata:      metadataPath,
			timestampMetadata: tsStr,
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
		webPMIME,
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
	switch strings.ToLower(filepath.Ext(path)) {
	case ".jpg", ".jpeg":
		contentType = jpegMIME
	case ".png":
		contentType = pngMIME
	default:
		s.Require().Fail("unknown image type")
	}

	return s.uploadToS3(
		ctx,
		s.env.s3SrcKeyBase+"/"+path,
		bodyPath,
		contentType,
		path,
		lastModified)
}

const (
	imagePathL            = "dir/image000.jpg"
	imagePathU            = "dir/image025.JPG"
	imageNonExistentPathL = "dir/nonexistent.jpg"
	imageNonExistentPathU = "dir/nonexistent.JPG"
)

func (s *ImgServerSuite) Test_AcceptedS3EFS_L() {
	s.AcceptedS3EFS(imagePathL)
}

func (s *ImgServerSuite) Test_AcceptedS3EFS_U() {
	s.AcceptedS3EFS(imagePathU)
}

func (s *ImgServerSuite) AcceptedS3EFS(path string) {
	eTag := s.uploadWebPToS3(s.ctx, path, sampleJPEGWebP, nil)

	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, chromeAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusOK, res.StatusCode)
		s.Assert().Equal(s.env.configure.permanentCache, header.cacheControl())
		s.Assert().Equal(webPMIME, header.contentType())
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

func (s *ImgServerSuite) Test_AcceptedS3NoEFS_L() {
	s.AcceptedS3NoEFS(imagePathL)
}

func (s *ImgServerSuite) Test_AcceptedS3NoEFS_U() {
	s.AcceptedS3NoEFS(imagePathU)
}

func (s *ImgServerSuite) AcceptedS3NoEFS(path string) {
	const longTextLen = int64(1024)

	s.uploadWebPToS3(s.ctx, path, sampleJPEGWebP, nil)
	s.uploadJPNGToS3(s.ctx, path, sampleJPEG, nil)
	s.Require().NoError(os.Remove(s.env.efsMountPath + "/" + path))

	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, chromeAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusNotFound, res.StatusCode)
		s.Assert().Equal(s.env.configure.temporaryCache, header.cacheControl())
		s.Assert().Equal(plainContentType, header.contentType())
		s.Assert().Greater(longTextLen, res.ContentLength)
		s.Assert().Equal("", header.eTag())
		s.Assert().Equal("", header.lastModified())
		body, err := ioutil.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		s.assertDelayedSQSMessage(ctx, path)
		// Ensure source file on S3 is also removed
		s.assertS3SrcNotExists(ctx, path)
	})
}

func (s *ImgServerSuite) Test_AcceptedNoS3EFS_L() {
	s.AcceptedNoS3EFS(imagePathL)
}

func (s *ImgServerSuite) Test_AcceptedNoS3EFS_U() {
	s.AcceptedNoS3EFS(imagePathU)
}

func (s *ImgServerSuite) AcceptedNoS3EFS(path string) {
	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, chromeAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusOK, res.StatusCode)
		s.Assert().Equal(s.env.configure.temporaryCache, header.cacheControl())
		s.Assert().Equal(jpegMIME, header.contentType())
		s.Assert().Equal(sampleJPEGSize, res.ContentLength)
		s.Assert().Equal(sampleJPEGETag, header.eTag())
		s.Assert().Equal(sampleLastModified, header.lastModified())
		body, err := ioutil.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		s.assertDelayedSQSMessage(ctx, path)
		s.assertS3SrcExists(ctx, path, &sampleModTime, jpegMIME, sampleJPEGSize)
	})
}

func (s *ImgServerSuite) Test_AcceptedNoS3NoEFS_L() {
	s.AcceptedNoS3NoEFS(imageNonExistentPathL)
}

func (s *ImgServerSuite) Test_AcceptedNoS3NoEFS_U() {
	s.AcceptedNoS3NoEFS(imageNonExistentPathU)
}

func (s *ImgServerSuite) AcceptedNoS3NoEFS(path string) {
	const longTextLen = int64(1024)

	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, chromeAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusNotFound, res.StatusCode)
		s.Assert().Equal(s.env.configure.temporaryCache, header.cacheControl())
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

func (s *ImgServerSuite) Test_UnacceptedS3EFS_L() {
	s.UnacceptedS3EFS(imagePathL)
}

func (s *ImgServerSuite) Test_UnacceptedS3EFS_U() {
	s.UnacceptedS3EFS(imagePathU)
}

func (s *ImgServerSuite) UnacceptedS3EFS(path string) {
	s.uploadWebPToS3(s.ctx, path, sampleJPEGWebP, nil)
	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, oldSafariAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusOK, res.StatusCode)
		s.Assert().Equal(s.env.configure.permanentCache, header.cacheControl())
		s.Assert().Equal(jpegMIME, header.contentType())
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

func (s *ImgServerSuite) Test_UnacceptedS3NoEFS_L() {
	s.UnacceptedS3NoEFS(imagePathL)
}

func (s *ImgServerSuite) Test_UnacceptedS3NoEFS_U() {
	s.UnacceptedS3NoEFS(imagePathU)
}

func (s *ImgServerSuite) UnacceptedS3NoEFS(path string) {
	const longTextLen = int64(1024)

	s.uploadWebPToS3(s.ctx, path, sampleJPEGWebP, nil)
	s.Require().NoError(os.Remove(s.env.efsMountPath + "/" + path))
	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, oldSafariAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusNotFound, res.StatusCode)
		s.Assert().Equal(s.env.configure.temporaryCache, header.cacheControl())
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

func (s *ImgServerSuite) Test_UnacceptedNoS3EFS_L() {
	s.UnacceptedNoS3EFS(imagePathL)
}

func (s *ImgServerSuite) Test_UnacceptedNoS3EFS_U() {
	s.UnacceptedNoS3EFS(imagePathU)
}

func (s *ImgServerSuite) UnacceptedNoS3EFS(path string) {
	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, oldSafariAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusOK, res.StatusCode)
		s.Assert().Equal(s.env.configure.permanentCache, header.cacheControl())
		s.Assert().Equal(jpegMIME, header.contentType())
		s.Assert().Equal(sampleJPEGSize, res.ContentLength)
		s.Assert().Equal(sampleJPEGETag, header.eTag())
		s.Assert().Equal(sampleLastModified, header.lastModified())
		body, err := ioutil.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		s.assertDelayedSQSMessage(ctx, path)
		s.assertS3SrcExists(ctx, path, &sampleModTime, jpegMIME, sampleJPEGSize)
	})
}

func (s *ImgServerSuite) Test_UnacceptedNoS3NoEFS_L() {
	s.UnacceptedNoS3NoEFS(imageNonExistentPathL)
}

func (s *ImgServerSuite) Test_UnacceptedNoS3NoEFS_U() {
	s.UnacceptedNoS3NoEFS(imageNonExistentPathU)
}

func (s *ImgServerSuite) UnacceptedNoS3NoEFS(path string) {
	const longTextLen = int64(1024)

	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, oldSafariAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusNotFound, res.StatusCode)
		s.Assert().Equal(s.env.configure.temporaryCache, header.cacheControl())
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

func (s *ImgServerSuite) Test_AcceptedS3EFSOld_L() {
	s.AcceptedS3EFSOld(imagePathL)
}

func (s *ImgServerSuite) Test_AcceptedS3EFSOld_U() {
	s.AcceptedS3EFSOld(imagePathU)
}

func (s *ImgServerSuite) AcceptedS3EFSOld(path string) {
	s.uploadWebPToS3(s.ctx, path, sampleJPEGWebP, &oldModTime)

	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, chromeAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusOK, res.StatusCode)
		s.Assert().Equal(s.env.configure.temporaryCache, header.cacheControl())
		s.Assert().Equal(jpegMIME, header.contentType())
		s.Assert().Equal(sampleJPEGSize, res.ContentLength)
		s.Assert().Equal(sampleJPEGETag, header.eTag())
		s.Assert().Equal(sampleLastModified, header.lastModified())
		body, err := ioutil.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		// Send message to update S3 object
		s.assertDelayedSQSMessage(ctx, path)
		s.assertS3SrcExists(ctx, path, &sampleModTime, jpegMIME, sampleJPEGSize)
	})
}

func (s *ImgServerSuite) Test_AcceptedNoS3EFSBatchSendRepeat() {
	s.serve(func(ctx context.Context, ts *httptest.Server) {
		for i := 0; i < 20; i++ {
			s.request(ctx, ts, fmt.Sprintf("/dir/image%03d.jpg", i), chromeAcceptHeader)
		}
		time.Sleep(3 * time.Second)

		msgs := s.receiveSQSMessages(ctx)
		s.Assert().Len(msgs, 20)
	})
}

func (s *ImgServerSuite) Test_AcceptedNoS3EFSBatchSendWait() {
	s.env.configure.sqsBatchWaitTime = 5

	s.serve(func(ctx context.Context, ts *httptest.Server) {
		for i := 0; i < 15; i++ {
			s.request(ctx, ts, fmt.Sprintf("/dir/image%03d.jpg", i), chromeAcceptHeader)
		}
		time.Sleep(3 * time.Second)

		msgs := s.receiveSQSMessages(ctx)
		s.Assert().Len(msgs, 10)
		s.deleteSQSMessages(ctx, msgs)

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
