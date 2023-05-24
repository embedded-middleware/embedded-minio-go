package model

import (
	"errors"
	"github.com/google/uuid"
	"sort"
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

// PutObject 存储对象
func (ms *MinioServer) PutObject(bucket, object, etag string, content []byte) error {
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
		Etag:         etag,
		Data:         content,
		LastModified: time.Now(),
	}
	return nil
}

// PutObjectPart 存储分片
func (ms *MinioServer) PutObjectPart(bucket, object, id, etag string, num int, content []byte) error {
	ms.Lock()
	defer ms.Unlock()

	var ok bool
	var bd *BucketData
	if bd, ok = ms.Buckets[bucket]; !ok {
		return errors.New("bucket not exists")
	}

	var oi *ObjectInfo
	if oi, ok = bd.Objects[object]; !ok {
		oi = &ObjectInfo{
			Name:        object,
			IsMultipart: true,
			UploadId:    id,
			Parts:       make(map[int]Multipart),
		}
		bd.Objects[object] = oi
	}

	if num != 0 && etag != "" {
		oi.Parts[num] = Multipart{
			Etag: etag,
			Data: content,
		}
	}

	return nil
}

// CompleteObjectPart 合并分片
func (ms *MinioServer) CompleteObjectPart(bucket, object, id string, parts *CompleteMultiPart) (string, error) {
	ms.Lock()
	defer ms.Unlock()

	var etag = GetUid()
	var ok bool
	var bd *BucketData
	if bd, ok = ms.Buckets[bucket]; !ok {
		return etag, errors.New("bucket not exists")
	}

	var oi *ObjectInfo
	if oi, ok = bd.Objects[object]; !ok {
		return etag, errors.New("object parts not exists")
	}

	if oi.UploadId != id {
		return etag, errors.New("object not exists")
	}

	sort.Sort(parts)
	for _, v := range parts.Parts {
		var part Multipart
		if part, ok = oi.Parts[v.PartNumber]; !ok {
			return etag, errors.New("object parts not exists")
		}
		if part.Etag != v.ETag {
			return etag, errors.New("object parts etag not same")
		}
		oi.Data = append(oi.Data, part.Data...)
	}
	oi.Etag = etag
	oi.Size = uint64(len(oi.Data))
	oi.LastModified = time.Now()
	return etag, nil
}

// DeleteObject 删除对象和分片
func (ms *MinioServer) DeleteObject(bucket, object string) error {
	ms.Lock()
	defer ms.Unlock()

	var ok bool
	var bd *BucketData
	if bd, ok = ms.Buckets[bucket]; !ok {
		return errors.New("bucket not exists")
	}

	if _, ok = bd.Objects[object]; !ok {
		return errors.New("object not exists")
	}
	delete(bd.Objects, object)
	return nil
}

// GetObject 获取对象
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

// GetUid 获取etag
func GetUid() string {
	var id string
	uid, err := uuid.NewUUID()
	if err != nil {
		return id
	}
	id = uid.String()
	return id
}
