package gominio

import (
	"context"
	"fmt"
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

	// test list bucket
	err = minioClient.MakeBucket(context.Background(), "test", minio.MakeBucketOptions{})
	require.NoError(t, err)

	err = minioClient.MakeBucket(context.Background(), "test1", minio.MakeBucketOptions{})
	require.NoError(t, err)

	err = minioClient.MakeBucket(context.Background(), "test", minio.MakeBucketOptions{})
	t.Log(err)
	require.NotNil(t, err)

	// test list bucket
	bi, err := minioClient.ListBuckets(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, len(bi))
	t.Log(bi)

	// test bucket location
	location, err := minioClient.GetBucketLocation(context.Background(), "test")
	require.NoError(t, err)
	require.Equal(t, "us-east-1", location)

	// test bucket policy
	policy := "test policy"
	err = minioClient.SetBucketPolicy(context.Background(), "test", policy)
	require.NoError(t, err)

	pl, err := minioClient.GetBucketPolicy(context.Background(), "test")
	require.NoError(t, err)
	require.Equal(t, policy, pl)

	pl, err = minioClient.GetBucketPolicy(context.Background(), "test1")
	require.NoError(t, err)
	require.Equal(t, "", pl)

	// test bucket exists
	ok, err := minioClient.BucketExists(context.Background(), "test")
	require.NoError(t, err)
	require.Equal(t, true, ok)

	ok, err = minioClient.BucketExists(context.Background(), "test1")
	require.NoError(t, err)
	require.Equal(t, true, ok)

	ok, err = minioClient.BucketExists(context.Background(), "test2")
	require.NoError(t, err)
	require.Equal(t, false, ok)

	// test bucket remove
	err = minioClient.RemoveBucket(context.Background(), "test")
	require.NoError(t, err)

	err = minioClient.RemoveBucket(context.Background(), "test")
	t.Log(err)
	require.NotNil(t, err)
}
