package gominio

import (
	"bytes"
	"context"
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

func TestServerStart(t *testing.T) {
	// Initialize server
	cfg := &ServerConfig{
		Port: 0,
	}
	srv, err := NewServer(cfg)
	require.NoError(t, err)

	// Start server
	port, err := srv.Start()
	require.NoError(t, err)
	defer srv.Close()

	require.Greater(t, port, 0)
}

func newServer(access, secret string) (*Server, error) {
	cfg := &ServerConfig{
		Access: access,
		Secret: secret,
		Port:   0,
	}
	srv, err := NewServer(cfg)
	if err != nil {
		return srv, err
	}

	// Start server
	port, err := srv.Start()
	srv.config.Port = port
	return srv, err
}

func TestBucket(t *testing.T) {
	server, err := newServer("minioadmin", "minioadmin")
	require.NoError(t, err)
	defer server.Close()

	// Initialize minio client object.
	minioClient, err := minio.New(fmt.Sprintf("127.0.0.1:%d", server.config.Port), &minio.Options{
		Creds: credentials.NewStaticV4("minioadmin", "minioadmin", ""),
	})
	require.NoError(t, err)

	// Test make bucket
	err = minioClient.MakeBucket(context.Background(), "test", minio.MakeBucketOptions{})
	require.NoError(t, err)

	err = minioClient.MakeBucket(context.Background(), "test1", minio.MakeBucketOptions{})
	require.NoError(t, err)

	err = minioClient.MakeBucket(context.Background(), "test", minio.MakeBucketOptions{})
	t.Log(err)
	require.NotNil(t, err)

	// Test list bucket
	bi, err := minioClient.ListBuckets(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, len(bi))
	t.Log(bi)

	// Test get bucket location
	location, err := minioClient.GetBucketLocation(context.Background(), "test")
	require.NoError(t, err)
	require.Equal(t, "us-east-1", location)

	// Test bucket policy
	policy := "test policy"
	err = minioClient.SetBucketPolicy(context.Background(), "test", policy)
	require.NoError(t, err)

	pl, err := minioClient.GetBucketPolicy(context.Background(), "test")
	require.NoError(t, err)
	require.Equal(t, policy, pl)

	pl, err = minioClient.GetBucketPolicy(context.Background(), "test1")
	require.NoError(t, err)
	require.Equal(t, "", pl)

	// Test bucket exists
	ok, err := minioClient.BucketExists(context.Background(), "test")
	require.NoError(t, err)
	require.Equal(t, true, ok)

	ok, err = minioClient.BucketExists(context.Background(), "test1")
	require.NoError(t, err)
	require.Equal(t, true, ok)

	ok, err = minioClient.BucketExists(context.Background(), "test2")
	require.NoError(t, err)
	require.Equal(t, false, ok)

	// Test bucket remove
	err = minioClient.RemoveBucket(context.Background(), "test")
	require.NoError(t, err)

	err = minioClient.RemoveBucket(context.Background(), "test")
	t.Log(err)
	require.NotNil(t, err)
}

func TestObject(t *testing.T) {
	server, err := newServer("minioadmin", "minioadmin")
	require.NoError(t, err)
	defer server.Close()

	// Initialize minio client object.
	minioClient, err := minio.New(fmt.Sprintf("127.0.0.1:%d", server.config.Port), &minio.Options{
		Creds: credentials.NewStaticV4("minioadmin", "minioadmin", ""),
	})
	require.NoError(t, err)

	// make bucket
	err = minioClient.MakeBucket(context.Background(), "test", minio.MakeBucketOptions{})
	require.NoError(t, err)

	// test put object
	content := `hello world`
	info, err := minioClient.PutObject(context.Background(), "test", "hello.txt",
		bytes.NewBuffer([]byte(content)), int64(len(content)), minio.PutObjectOptions{})
	require.NoError(t, err)
	t.Log(info)

	// test get object
	oi, err := minioClient.GetObject(context.Background(), "test", "hello1.txt", minio.GetObjectOptions{})
	require.NoError(t, err)
	_, err = io.ReadAll(oi)
	require.NotNil(t, err)
	t.Log(err)

	oi, err = minioClient.GetObject(context.Background(), "test", "hello.txt", minio.GetObjectOptions{})
	require.NoError(t, err)
	data, err := io.ReadAll(oi)
	require.NoError(t, err)
	require.Equal(t, content, string(data))
	t.Log(string(data))

	// test delete object
	err = minioClient.RemoveObject(context.Background(), "test", "hello1.txt", minio.RemoveObjectOptions{})
	require.NotNil(t, err)
	t.Log(err)

	err = minioClient.RemoveObject(context.Background(), "test", "hello.txt", minio.RemoveObjectOptions{})
	require.NoError(t, err)

	oi, err = minioClient.GetObject(context.Background(), "test", "hello1.txt", minio.GetObjectOptions{})
	require.NoError(t, err)
	_, err = io.ReadAll(oi)
	require.NotNil(t, err)
	t.Log(err)
}

func TestObjectOther(t *testing.T) {
	server, err := newServer("minioadmin", "minioadmin")
	require.NoError(t, err)

	// Initialize minio client object.
	minioClient, err := minio.New(fmt.Sprintf("127.0.0.1:%d", server.config.Port), &minio.Options{
		Creds: credentials.NewStaticV4("minioadmin", "minioadmin", ""),
	})
	require.NoError(t, err)

	// prepare bucket object
	err = minioClient.MakeBucket(context.Background(), "test", minio.MakeBucketOptions{})
	require.NoError(t, err)

	content := `hello world`
	info, err := minioClient.PutObject(context.Background(), "test", "hello.txt",
		bytes.NewBuffer([]byte(content)), int64(len(content)), minio.PutObjectOptions{})
	require.NoError(t, err)
	t.Log(info)

	// put object tag
	tag, err := tags.MapToObjectTags(map[string]string{
		"key": "value",
	})
	require.NoError(t, err)
	err = minioClient.PutObjectTagging(context.Background(), "test", "hello.txt", tag,
		minio.PutObjectTaggingOptions{})
	require.NoError(t, err)

	// get object tag
	var tagx *tags.Tags
	tagx, err = minioClient.GetObjectTagging(context.Background(), "test", "hello.txt",
		minio.GetObjectTaggingOptions{})
	require.NoError(t, err)
	require.Equal(t, tag.String(), tagx.String())
	t.Log(tagx.String())

	// delete object tag
	err = minioClient.RemoveObjectTagging(context.Background(), "test", "hello.txt",
		minio.RemoveObjectTaggingOptions{})
	require.NoError(t, err)

	tagx, err = minioClient.GetObjectTagging(context.Background(), "test", "hello.txt",
		minio.GetObjectTaggingOptions{})
	require.NoError(t, err)
	t.Log("after remove tag: ", tagx.String())
}
