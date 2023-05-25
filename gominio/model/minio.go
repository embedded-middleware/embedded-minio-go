package model

import (
	"bytes"
	"encoding/xml"
	"sync"
	"time"
)

var ms *MinioServer

func InitMinioServer(access, secret string) {
	ms = &MinioServer{
		Access:  access,
		Secret:  secret,
		Buckets: make(map[string]*BucketData),
	}
}

func GetMS() *MinioServer {
	return ms
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
