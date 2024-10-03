package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

const (
	cssMIME          = "text/css; charset=utf-8"
	jpegMIME         = "image/jpeg"
	plainContentType = "text/plain; charset=utf-8"
	pngMIME          = "image/png"
	webPMIME         = "image/webp"
)

type ImgServerSuite struct {
	*TestSuite
}

func toWebPPath(path string) string {
	return path + ".webp"
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
	copy(sampleCSS, fmt.Sprintf("%s/dir/style.css", s.env.efsMountPath), &s.Suite)
	copy(sampleMinCSS, fmt.Sprintf("%s/dir/style.min.css", s.env.efsMountPath), &s.Suite)
	copy(sampleNominifyCSS, fmt.Sprintf("%s/dir/nominify.css", s.env.efsMountPath), &s.Suite)
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
	s.Assert().Equal(2, t.Version)
	s.Assert().Equal(path, t.Path)
	s.Assert().Equal(s.env.s3Bucket, t.Src.Bucket)
	s.Assert().Equal(s.env.s3SrcPrefix, t.Src.Prefix)
	s.Assert().Equal(s.env.s3Bucket, t.Dest.Bucket)
	s.Assert().Equal(s.env.s3DestPrefix, t.Dest.Prefix)
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
		Key:    aws.String(s.env.s3SrcPrefix + path),
	})
	s.Assert().NoError(err)
	s.Assert().Equal(path, res.Metadata[pathMetadata])
	t, err := time.Parse(time.RFC3339Nano, res.Metadata[timestampMetadata])
	s.Assert().NoError(err)
	s.Assert().Equal(lastModified.UTC(), t)
	s.Assert().Equal(contentType, *res.ContentType)
	s.Assert().Equal(contentLength, *res.ContentLength)
}

func (s *ImgServerSuite) assertS3SrcNotExists(ctx context.Context, path string) {
	_, err := s.env.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &s.env.s3Bucket,
		Key:    aws.String(s.env.s3SrcPrefix + path),
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

func (s *ImgServerSuite) uploadToPublicContentS3(
	ctx context.Context,
	key string,
	bodyPath string,
	contentType, cacheControl string,
) string {
	f, err := os.Open(bodyPath)
	s.Require().NoError(err)
	defer func() {
		s.Require().NoError(f.Close())
	}()

	res, err := s.env.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:       &s.env.publicContentS3Bucket,
		Key:          &key,
		Body:         f,
		ContentType:  &contentType,
		CacheControl: &cacheControl,
	})
	s.Require().NoError(err)

	return *res.ETag
}

func (s *ImgServerSuite) contentType(path string) string {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".jpg", ".jpeg":
		return jpegMIME
	case ".png":
		return pngMIME
	case ".webp":
		return webPMIME
	case ".css":
		return cssMIME
	default:
		s.Require().Fail("unknown image type")
	}
	return ""
}

func (s *ImgServerSuite) uploadFileToS3Dest(
	ctx context.Context,
	path, s3Path, bodyPath string,
	lastModified *time.Time,
) string {
	return s.uploadToS3(
		ctx,
		s.env.s3DestPrefix+s3Path,
		bodyPath,
		s.contentType(s3Path),
		path,
		lastModified)
}

func (s *ImgServerSuite) uploadFileToS3Src(
	ctx context.Context,
	path, s3Path, bodyPath string,
	lastModified *time.Time,
) string {
	return s.uploadToS3(
		ctx,
		s.env.s3SrcPrefix+s3Path,
		bodyPath,
		s.contentType(s3Path),
		path,
		lastModified)
}

const (
	jpgPathL             = "dir/image000.jpg"
	jpgPathU             = "dir/image025.JPG"
	jpgNonExistentPathL  = "dir/nonexistent.jpg"
	jpgNonExistentPathU  = "dir/nonexistent.JPG"
	jpgNotADirectoryPath = "dir/image000.jpg/foo.jpg"
	cssPathL             = "dir/style.css"
	cssNonExistentPathL  = "dir/nonexistent.css"
	minCSSPathL          = "dir/style.min.css"
	nominifyCSSPathL     = "dir/nominify.css"

	publicContentCacheControl = "public, max-age=86400"
)

var jpgTooLongPath = "dir/" + strings.Repeat("nonexistent", 100) + ".jpg"

func (s *ImgServerSuite) Test_JPGAcceptedS3EFS_L() {
	s.JPGAcceptedS3EFS(jpgPathL)
}

func (s *ImgServerSuite) Test_JPGAcceptedS3EFS_U() {
	s.JPGAcceptedS3EFS(jpgPathU)
}

func (s *ImgServerSuite) JPGAcceptedS3EFS(path string) {
	eTag := s.uploadFileToS3Dest(s.ctx, path, toWebPPath(path), sampleJPEGWebP, nil)

	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, chromeAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusOK, res.StatusCode)
		s.Assert().Equal(s.env.configure.permanentCache.value, header.cacheControl())
		s.Assert().Equal(webPMIME, header.contentType())
		s.Assert().Equal(sampleJPEGWebPSize, res.ContentLength)
		s.Assert().Equal(eTag, header.eTag())
		s.Assert().Equal(sampleLastModified, header.lastModified())
		body, err := io.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		s.assertNoSQSMessage(ctx)
		s.assertS3SrcNotExists(ctx, path)
	})
}

func (s *ImgServerSuite) Test_PublicContentJPG() {
	keyPrefix := strings.Split(s.env.publicContentPathPatterns, "/")[1]
	path := keyPrefix + "/wp-content/uploads/sample.jpg"
	eTag := s.uploadToPublicContentS3(
		s.ctx,
		path,
		sampleJPEG,
		jpegMIME,
		publicContentCacheControl)

	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, chromeAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusOK, res.StatusCode)
		s.Assert().Equal(publicContentCacheControl, header.cacheControl())
		s.Assert().Equal(jpegMIME, header.contentType())
		s.Assert().Equal(sampleJPEGSize, res.ContentLength)
		s.Assert().Equal(eTag, header.eTag())
		body, err := io.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))
	})
}

func (s *ImgServerSuite) Test_BypassMinifier() {
	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+nominifyCSSPathL, chromeAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusOK, res.StatusCode)
		s.Assert().Equal(s.env.configure.permanentCache.value, header.cacheControl())
		s.Assert().Equal(cssMIME, header.contentType())
		s.Assert().Equal(sampleNominifyCSSSize, res.ContentLength)
		s.Assert().Equal(sampleNominifyCSSETag, header.eTag())
		s.Assert().Equal(sampleLastModified, header.lastModified())
		body, err := io.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		s.assertNoSQSMessage(ctx)
		s.assertS3SrcNotExists(ctx, nominifyCSSPathL)
	})
}

func (s *ImgServerSuite) Test_JPGAcceptedS3NoEFS_L() {
	s.JPGAcceptedS3NoEFS(jpgPathL)
}

func (s *ImgServerSuite) Test_JPGAcceptedS3NoEFS_U() {
	s.JPGAcceptedS3NoEFS(jpgPathU)
}

func (s *ImgServerSuite) JPGAcceptedS3NoEFS(path string) {
	const longTextLen = int64(1024)

	s.uploadFileToS3Dest(s.ctx, path, toWebPPath(path), sampleJPEGWebP, nil)
	s.uploadFileToS3Src(s.ctx, path, path, sampleJPEG, nil)
	s.Require().NoError(os.Remove(s.env.efsMountPath + "/" + path))

	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, chromeAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusNotFound, res.StatusCode)
		s.Assert().Equal(s.env.configure.temporaryCache.value, header.cacheControl())
		s.Assert().Equal(plainContentType, header.contentType())
		s.Assert().Greater(longTextLen, res.ContentLength)
		s.Assert().Equal("", header.eTag())
		s.Assert().Equal("", header.lastModified())
		body, err := io.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		s.assertDelayedSQSMessage(ctx, path)
		// Ensure source file on S3 is also removed
		s.assertS3SrcNotExists(ctx, path)
	})
}

func (s *ImgServerSuite) Test_JPGAcceptedNoS3EFS_L() {
	s.JPGAcceptedNoS3EFS(jpgPathL)
}

func (s *ImgServerSuite) Test_JPGAcceptedNoS3EFS_U() {
	s.JPGAcceptedNoS3EFS(jpgPathU)
}

func (s *ImgServerSuite) JPGAcceptedNoS3EFS(path string) {
	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, chromeAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusOK, res.StatusCode)
		s.Assert().Equal(s.env.configure.temporaryCache.value, header.cacheControl())
		s.Assert().Equal(jpegMIME, header.contentType())
		s.Assert().Equal(sampleJPEGSize, res.ContentLength)
		s.Assert().Equal(sampleJPEGETag, header.eTag())
		s.Assert().Equal(sampleLastModified, header.lastModified())
		body, err := io.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		s.assertDelayedSQSMessage(ctx, path)
		s.assertS3SrcExists(ctx, path, &sampleModTime, jpegMIME, sampleJPEGSize)
	})
}

func (s *ImgServerSuite) Test_JPGAcceptedNoS3NoEFS_L() {
	s.JPGAcceptedNoS3NoEFS(jpgNonExistentPathL)
}

func (s *ImgServerSuite) Test_JPGAcceptedNoS3NoEFS_U() {
	s.JPGAcceptedNoS3NoEFS(jpgNonExistentPathU)
}

func (s *ImgServerSuite) Test_JPGAcceptedNoS3NoEFS_NotADir() {
	s.JPGAcceptedNoS3NoEFS(jpgNotADirectoryPath)
}

func (s *ImgServerSuite) JPGAcceptedNoS3NoEFS(path string) {
	const longTextLen = int64(1024)

	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, chromeAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusNotFound, res.StatusCode)
		s.Assert().Equal(s.env.configure.temporaryCache.value, header.cacheControl())
		s.Assert().Equal(plainContentType, header.contentType())
		s.Assert().Greater(longTextLen, res.ContentLength)
		s.Assert().Equal("", header.eTag())
		s.Assert().Equal("", header.lastModified())
		body, err := io.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		s.assertNoSQSMessage(ctx)
		s.assertS3SrcNotExists(ctx, path)
	})
}

func (s *ImgServerSuite) Test_JPGAcceptedNoS3NoEFS_TooLong() {
	const longTextLen = int64(1024)

	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+jpgTooLongPath, chromeAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusNotFound, res.StatusCode)
		s.Assert().Equal(s.env.configure.temporaryCache.value, header.cacheControl())
		s.Assert().Equal(plainContentType, header.contentType())
		s.Assert().Greater(longTextLen, res.ContentLength)
		s.Assert().Equal("", header.eTag())
		s.Assert().Equal("", header.lastModified())
		body, err := io.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		s.assertNoSQSMessage(ctx)
	})
}

func (s *ImgServerSuite) Test_JPGUnacceptedS3EFS_L() {
	s.JPGUnacceptedS3EFS(jpgPathL)
}

func (s *ImgServerSuite) Test_JPGUnacceptedS3EFS_U() {
	s.JPGUnacceptedS3EFS(jpgPathU)
}

func (s *ImgServerSuite) JPGUnacceptedS3EFS(path string) {
	s.uploadFileToS3Dest(s.ctx, path, toWebPPath(path), sampleJPEGWebP, nil)
	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, oldSafariAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusOK, res.StatusCode)
		s.Assert().Equal(s.env.configure.permanentCache.value, header.cacheControl())
		s.Assert().Equal(jpegMIME, header.contentType())
		s.Assert().Equal(sampleJPEGSize, res.ContentLength)
		s.Assert().Equal(sampleJPEGETag, header.eTag())
		s.Assert().Equal(sampleLastModified, header.lastModified())
		body, err := io.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		s.assertNoSQSMessage(ctx)
		s.assertS3SrcNotExists(ctx, path)
	})
}

func (s *ImgServerSuite) Test_JPGUnacceptedS3NoEFS_L() {
	s.JPGUnacceptedS3NoEFS(jpgPathL)
}

func (s *ImgServerSuite) Test_JPGUnacceptedS3NoEFS_U() {
	s.JPGUnacceptedS3NoEFS(jpgPathU)
}

func (s *ImgServerSuite) JPGUnacceptedS3NoEFS(path string) {
	const longTextLen = int64(1024)

	s.uploadFileToS3Dest(s.ctx, path, toWebPPath(path), sampleJPEGWebP, nil)
	s.Require().NoError(os.Remove(s.env.efsMountPath + "/" + path))
	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, oldSafariAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusNotFound, res.StatusCode)
		s.Assert().Equal(s.env.configure.temporaryCache.value, header.cacheControl())
		s.Assert().Equal(plainContentType, header.contentType())
		s.Assert().Greater(longTextLen, res.ContentLength)
		s.Assert().Equal("", header.eTag())
		s.Assert().Equal("", header.lastModified())
		body, err := io.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		s.assertDelayedSQSMessage(ctx, path)
		s.assertS3SrcNotExists(ctx, path)
	})
}

func (s *ImgServerSuite) Test_JPGUnacceptedNoS3EFS_L() {
	s.JPGUnacceptedNoS3EFS(jpgPathL)
}

func (s *ImgServerSuite) Test_JPGUnacceptedNoS3EFS_U() {
	s.JPGUnacceptedNoS3EFS(jpgPathU)
}

func (s *ImgServerSuite) JPGUnacceptedNoS3EFS(path string) {
	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, oldSafariAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusOK, res.StatusCode)
		s.Assert().Equal(s.env.configure.permanentCache.value, header.cacheControl())
		s.Assert().Equal(jpegMIME, header.contentType())
		s.Assert().Equal(sampleJPEGSize, res.ContentLength)
		s.Assert().Equal(sampleJPEGETag, header.eTag())
		s.Assert().Equal(sampleLastModified, header.lastModified())
		body, err := io.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		s.assertDelayedSQSMessage(ctx, path)
		s.assertS3SrcExists(ctx, path, &sampleModTime, jpegMIME, sampleJPEGSize)
	})
}

func (s *ImgServerSuite) Test_JPGUnacceptedNoS3NoEFS_L() {
	s.JPGUnacceptedNoS3NoEFS(jpgNonExistentPathL)
}

func (s *ImgServerSuite) Test_JPGUnacceptedNoS3NoEFS_U() {
	s.JPGUnacceptedNoS3NoEFS(jpgNonExistentPathU)
}

func (s *ImgServerSuite) JPGUnacceptedNoS3NoEFS(path string) {
	const longTextLen = int64(1024)

	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, oldSafariAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusNotFound, res.StatusCode)
		s.Assert().Equal(s.env.configure.temporaryCache.value, header.cacheControl())
		s.Assert().Equal(plainContentType, header.contentType())
		s.Assert().Greater(longTextLen, res.ContentLength)
		s.Assert().Equal("", header.eTag())
		s.Assert().Equal("", header.lastModified())
		body, err := io.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		s.assertNoSQSMessage(ctx)
		s.assertS3SrcNotExists(ctx, path)
	})
}

func (s *ImgServerSuite) Test_JPGAcceptedS3EFSOld_L() {
	s.JPGAcceptedS3EFSOld(jpgPathL)
}

func (s *ImgServerSuite) Test_JPGAcceptedS3EFSOld_U() {
	s.JPGAcceptedS3EFSOld(jpgPathU)
}

func (s *ImgServerSuite) JPGAcceptedS3EFSOld(path string) {
	s.uploadFileToS3Dest(s.ctx, path, toWebPPath(path), sampleJPEGWebP, &oldModTime)

	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, chromeAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusOK, res.StatusCode)
		s.Assert().Equal(s.env.configure.temporaryCache.value, header.cacheControl())
		s.Assert().Equal(jpegMIME, header.contentType())
		s.Assert().Equal(sampleJPEGSize, res.ContentLength)
		s.Assert().Equal(sampleJPEGETag, header.eTag())
		s.Assert().Equal(sampleLastModified, header.lastModified())
		body, err := io.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		// Send message to update S3 object
		s.assertDelayedSQSMessage(ctx, path)
		s.assertS3SrcExists(ctx, path, &sampleModTime, jpegMIME, sampleJPEGSize)
	})
}

func (s *ImgServerSuite) Test_JPGAcceptedNoS3EFSBatchSendRepeat() {
	s.serve(func(ctx context.Context, ts *httptest.Server) {
		for i := 0; i < 20; i++ {
			s.request(ctx, ts, fmt.Sprintf("/dir/image%03d.jpg", i), chromeAcceptHeader)
		}
		time.Sleep(3 * time.Second)

		msgs := s.receiveSQSMessages(ctx)
		s.Assert().Len(msgs, 20)
	})
}

func (s *ImgServerSuite) Test_JPGAcceptedNoS3EFSBatchSendWait() {
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

	oldBytes, err := os.ReadFile(oldLogPath)
	s.Require().NoError(err)

	currentBytes, err := os.ReadFile(currentLogPath)
	s.Require().NoError(err)

	oldLog := string(oldBytes)
	currentLog := string(currentBytes)

	s.Assert().Contains(oldLog, "first")
	s.Assert().Contains(oldLog, "second")

	s.Assert().Contains(currentLog, "third")
}

func (s *ImgServerSuite) Test_CSSS3EFS() {
	eTag := s.uploadFileToS3Dest(s.ctx, cssPathL, cssPathL, sampleMinCSS, nil)

	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+cssPathL, chromeAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusOK, res.StatusCode)
		s.Assert().Equal(s.env.configure.permanentCache.value, header.cacheControl())
		s.Assert().Equal(cssMIME, header.contentType())
		s.Assert().Equal(sampleMinCSSSize, res.ContentLength)
		s.Assert().Equal(eTag, header.eTag())
		s.Assert().Equal(sampleLastModified, header.lastModified())
		body, err := io.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		s.assertNoSQSMessage(ctx)
		s.assertS3SrcNotExists(ctx, cssPathL)
	})
}

func (s *ImgServerSuite) Test_CSSS3NoEFS() {
	const longTextLen = int64(1024)

	s.uploadFileToS3Dest(s.ctx, cssPathL, cssPathL, sampleMinCSS, nil)
	s.uploadFileToS3Src(s.ctx, cssPathL, cssPathL, sampleCSS, nil)
	s.Require().NoError(os.Remove(s.env.efsMountPath + "/" + cssPathL))

	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+cssPathL, chromeAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusNotFound, res.StatusCode)
		s.Assert().Equal(s.env.configure.temporaryCache.value, header.cacheControl())
		s.Assert().Equal(plainContentType, header.contentType())
		s.Assert().Greater(longTextLen, res.ContentLength)
		s.Assert().Equal("", header.eTag())
		s.Assert().Equal("", header.lastModified())
		body, err := io.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		s.assertDelayedSQSMessage(ctx, cssPathL)
		// Ensure source file on S3 is also removed
		s.assertS3SrcNotExists(ctx, cssPathL)
	})
}

func (s *ImgServerSuite) Test_CSSNoS3EFS() {
	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+cssPathL, chromeAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusOK, res.StatusCode)
		s.Assert().Equal(s.env.configure.temporaryCache.value, header.cacheControl())
		s.Assert().Equal(cssMIME, header.contentType())
		s.Assert().Equal(sampleCSSSize, res.ContentLength)
		s.Assert().Equal(sampleCSSETag, header.eTag())
		s.Assert().Equal(sampleLastModified, header.lastModified())
		body, err := io.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		s.assertDelayedSQSMessage(ctx, cssPathL)
		s.assertS3SrcExists(ctx, cssPathL, &sampleModTime, cssMIME, sampleCSSSize)
	})
}

func (s *ImgServerSuite) Test_CSSNoS3NoEFS() {
	const longTextLen = int64(1024)

	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+cssNonExistentPathL, chromeAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusNotFound, res.StatusCode)
		s.Assert().Equal(s.env.configure.temporaryCache.value, header.cacheControl())
		s.Assert().Equal(plainContentType, header.contentType())
		s.Assert().Greater(longTextLen, res.ContentLength)
		s.Assert().Equal("", header.eTag())
		s.Assert().Equal("", header.lastModified())
		body, err := io.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		s.assertNoSQSMessage(ctx)
		s.assertS3SrcNotExists(ctx, cssNonExistentPathL)
	})
}

func (s *ImgServerSuite) Test_CSSS3EFSOld() {
	s.uploadFileToS3Dest(s.ctx, cssPathL, toWebPPath(cssPathL), sampleMinCSS, &oldModTime)

	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+cssPathL, chromeAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusOK, res.StatusCode)
		s.Assert().Equal(s.env.configure.temporaryCache.value, header.cacheControl())
		s.Assert().Equal(cssMIME, header.contentType())
		s.Assert().Equal(sampleCSSSize, res.ContentLength)
		s.Assert().Equal(sampleCSSETag, header.eTag())
		s.Assert().Equal(sampleLastModified, header.lastModified())
		body, err := io.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		// Send message to update S3 object
		s.assertDelayedSQSMessage(ctx, cssPathL)
		s.assertS3SrcExists(ctx, cssPathL, &sampleModTime, cssMIME, sampleCSSSize)
	})
}

func (s *ImgServerSuite) Test_MinCSSS3EFS() {
	s.FileS3EFS(minCSSPathL, cssMIME, sampleMinCSSSize, sampleMinCSSETag)
}

func (s *ImgServerSuite) FileS3EFS(
	path string,
	contentType string,
	size int64,
	eTag string,
) {
	s.uploadFileToS3Dest(s.ctx, path, path, sampleJPEG, nil)

	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, chromeAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusOK, res.StatusCode)
		s.Assert().Equal(s.env.configure.permanentCache.value, header.cacheControl())
		s.Assert().Equal(contentType, header.contentType())
		s.Assert().Equal(size, res.ContentLength)
		s.Assert().Equal(eTag, header.eTag())
		s.Assert().Equal(sampleLastModified, header.lastModified())
		body, err := io.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		s.assertNoSQSMessage(ctx)
		s.assertS3SrcNotExists(ctx, path)
	})
}

func (s *ImgServerSuite) Test_MinCSSS3NoEFS() {
	s.FileS3NoEFS(minCSSPathL)
}

func (s *ImgServerSuite) FileS3NoEFS(path string) {
	const longTextLen = int64(1024)

	s.uploadFileToS3Dest(s.ctx, path, path, sampleJPEG, nil)
	s.Require().NoError(os.Remove(s.env.efsMountPath + "/" + path))

	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, chromeAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusNotFound, res.StatusCode)
		s.Assert().Equal(s.env.configure.temporaryCache.value, header.cacheControl())
		s.Assert().Equal(plainContentType, header.contentType())
		s.Assert().Greater(longTextLen, res.ContentLength)
		s.Assert().Equal("", header.eTag())
		s.Assert().Equal("", header.lastModified())
		body, err := io.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		s.assertNoSQSMessage(ctx)
		s.assertS3SrcNotExists(ctx, path)
	})
}

func (s *ImgServerSuite) Test_MinCSSNoS3EFS() {
	s.FileNoS3EFS(minCSSPathL, cssMIME, sampleMinCSSSize, sampleMinCSSETag)
}

func (s *ImgServerSuite) FileNoS3EFS(
	path string,
	contentType string,
	size int64,
	eTag string,
) {
	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, chromeAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusOK, res.StatusCode)
		s.Assert().Equal(s.env.configure.permanentCache.value, header.cacheControl())
		s.Assert().Equal(contentType, header.contentType())
		s.Assert().Equal(size, res.ContentLength)
		s.Assert().Equal(eTag, header.eTag())
		s.Assert().Equal(sampleLastModified, header.lastModified())
		body, err := io.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		s.assertNoSQSMessage(ctx)
		s.assertS3SrcNotExists(ctx, path)
	})
}

func (s *ImgServerSuite) Test_MinCSSNoS3NoEFS() {
	s.FileNoS3NoEFS(minCSSPathL)
}

func (s *ImgServerSuite) FileNoS3NoEFS(path string) {
	const longTextLen = int64(1024)

	s.Require().NoError(os.Remove(s.env.efsMountPath + "/" + path))

	s.serve(func(ctx context.Context, ts *httptest.Server) {
		res := s.request(ctx, ts, "/"+path, chromeAcceptHeader)

		header := httpHeader(*res)
		s.Assert().Equal(http.StatusNotFound, res.StatusCode)
		s.Assert().Equal(s.env.configure.temporaryCache.value, header.cacheControl())
		s.Assert().Equal(plainContentType, header.contentType())
		s.Assert().Greater(longTextLen, res.ContentLength)
		s.Assert().Equal("", header.eTag())
		s.Assert().Equal("", header.lastModified())
		body, err := io.ReadAll(res.Body)
		s.Assert().NoError(err)
		s.Assert().Len(body, int(res.ContentLength))

		s.assertNoSQSMessage(ctx)
		s.assertS3SrcNotExists(ctx, path)
	})
}
