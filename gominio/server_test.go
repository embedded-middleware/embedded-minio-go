package gominio

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
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
	//defer server.Close()

	// Initialize minio client object.
	minioClient, err := minio.New(fmt.Sprintf("127.0.0.1:%d", server.config.Port), &minio.Options{
		Creds: credentials.NewStaticV4("minioadmin", "minioadmin", ""),
	})
	require.NoError(t, err)

	err = minioClient.MakeBucket(context.Background(), "test", minio.MakeBucketOptions{})
	require.NoError(t, err)

	err = minioClient.MakeBucket(context.Background(), "test", minio.MakeBucketOptions{})
	t.Log(err)
	require.NotNil(t, err)

	location, err := minioClient.GetBucketLocation(context.Background(), "test")
	require.NoError(t, err)
	require.Equal(t, "us-east-1", location)

	ok, err := minioClient.BucketExists(context.Background(), "test")
	require.NoError(t, err)
	require.Equal(t, true, ok)

	ok, err = minioClient.BucketExists(context.Background(), "test1")
	require.NoError(t, err)
	require.Equal(t, false, ok)

	err = minioClient.RemoveBucket(context.Background(), "test")
	require.NoError(t, err)

	err = minioClient.RemoveBucket(context.Background(), "test")
	t.Log(err)
	require.NotNil(t, err)
}

func TestObject(t *testing.T) {
	server, err := newServer("minioadmin", "minioadmin")
	require.NoError(t, err)
	//defer server.Close()

	// Initialize minio client object.
	minioClient, err := minio.New(fmt.Sprintf("127.0.0.1:%d", server.config.Port), &minio.Options{
		Creds: credentials.NewStaticV4("minioadmin", "minioadmin", ""),
	})

	err = minioClient.MakeBucket(context.Background(), "test", minio.MakeBucketOptions{})
	require.NoError(t, err)

	content := `hello world`
	info, err := minioClient.PutObject(context.Background(), "test", "hello.txt",
		bytes.NewBuffer([]byte(content)), int64(len(content)), minio.PutObjectOptions{})
	require.NoError(t, err)
	t.Log(info)

	oi, err := minioClient.GetObject(context.Background(), "test", "hello1.txt",
		minio.GetObjectOptions{})
	require.NoError(t, err)
	_, err = io.ReadAll(oi)
	require.NotNil(t, err)
	t.Log(err)

	oi, err = minioClient.GetObject(context.Background(), "test", "hello.txt",
		minio.GetObjectOptions{})
	require.NoError(t, err)
	data, err := io.ReadAll(oi)
	require.NoError(t, err)
	t.Log(string(data))

	info, err = minioClient.FPutObject(context.Background(), "test", "volcano.zip",
		"D:\\go\\codes\\sorce code\\volcano-1.7.0.zip", minio.PutObjectOptions{PartSize: 1024 * 1024 * 5})
	require.NoError(t, err)
	t.Log(info)

	err = minioClient.FGetObject(context.Background(), "test", "volcano.zip",
		"D:\\go\\codes\\sorce code\\volcano.zip", minio.GetObjectOptions{})
	require.NoError(t, err)
}
