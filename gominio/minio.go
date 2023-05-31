package gominio

import (
	"bytes"
	"encoding/xml"
	"github.com/minio/minio-go/v7/pkg/tags"
	"io"
	"sync"
	"time"
)

func NewMinioServer(access, secret string) *MinioServer {
	minio := &MinioServer{
		Access:  access,
		Secret:  secret,
		Buckets: make(map[string]*BucketData),
	}
	return minio
}

type MinioServer struct {
	sync.RWMutex
	Access  string
	Secret  string
	Buckets map[string]*BucketData
}

type BucketData struct {
	Info    BucketInfo
	Objects map[string]*ObjectInfo
}

type BucketInfo struct {
	Quota   uint64
	Used    uint64
	Policy  string
	Created time.Time
}

type ObjectInfo struct {
	Name string
	Size uint64
	Etag string
	Data []byte
	Tags *tags.Tags

	IsMultipart bool
	UploadId    string
	Parts       map[int]Multipart

	LastModified time.Time
}

type Multipart struct {
	Etag string
	Data []byte
}

func encodeAny(v any) []byte {
	var bytesBuffer bytes.Buffer
	bytesBuffer.WriteString(xml.Header)
	e := xml.NewEncoder(&bytesBuffer)
	err := e.Encode(v)
	if err != nil {
		return nil
	}
	return bytesBuffer.Bytes()
}

func decodeAny(r io.Reader, v any) {
	err := xml.NewDecoder(r).Decode(v)
	if err != nil {
		return
	}
}
