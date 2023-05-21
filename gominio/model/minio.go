package model

import (
	"errors"
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
	Quota uint64
	Used  uint64
}

type ObjectInfo struct {
	Name         string
	Size         uint64
	Data         string
	LastModified time.Time
}

// BucketExists 存储桶是否存在
func (ms *MinioServer) BucketExists(bucket string) bool {
	ms.RLock()
	defer ms.RUnlock()

	_, ok := ms.Buckets[bucket]
	return ok
}

// MakeBucket 创建bucket
func (ms *MinioServer) MakeBucket(bucket string) bool {
	ms.Lock()
	defer ms.Unlock()
	if _, ok := ms.Buckets[bucket]; ok {
		return false
	}
	ms.Buckets[bucket] = &BucketData{
		Objects: make(map[string]*ObjectInfo),
	}
	return true
}

// DelBucket 删除bucket
func (ms *MinioServer) DelBucket(bucket string, force bool) error {
	ms.Lock()
	defer ms.Unlock()
	if _, ok := ms.Buckets[bucket]; !ok {
		return nil
	}

	// 如果不是强制删除, 那么如果存储桶有object存在，那么就会删除失败
	if bd, ok := ms.Buckets[bucket]; ok && len(bd.Objects) > 0 && !force {
		return errors.New("bucket not empty")
	}
	delete(ms.Buckets, bucket)
	return nil
}

func (ms *MinioServer) PutObject(bucket, object, content string) error {
	ms.Lock()
	defer ms.Unlock()

	var ok bool
	var bd *BucketData
	if bd, ok = ms.Buckets[bucket]; !ok {
		return errors.New("bucket not exists")
	}

	bd.Objects[object] = &ObjectInfo{
		Name:         object,
		Size:         uint64(len(content)),
		Data:         content,
		LastModified: time.Now(),
	}
	return nil
}

func (ms *MinioServer) GetObject(bucket, object string) (*ObjectInfo, error) {
	ms.RLock()
	defer ms.RUnlock()

	var (
		bd *BucketData
		oi *ObjectInfo
		ok bool
	)

	if bd, ok = ms.Buckets[bucket]; !ok {
		return nil, errors.New("bucket not exists")
	}

	oi, ok = bd.Objects[object]
	if !ok {
		return nil, errors.New("object not exists")
	}

	return oi, nil
}
