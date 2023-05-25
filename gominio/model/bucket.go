package model

import (
	"encoding/xml"
	"errors"
	"time"
)

// BucketExists 存储桶是否存在
func (ms *MinioServer) BucketExists(bucket string) bool {
	ms.RLock()
	defer ms.RUnlock()

	_, ok := ms.Buckets[bucket]
	return ok
}

// ListBucket list bucket
func (ms *MinioServer) ListBucket() ListBucketsResponse {
	ms.RLock()
	defer ms.RUnlock()

	lr := ListBucketsResponse{}
	for bk, info := range ms.Buckets {
		lr.Buckets.Buckets = append(lr.Buckets.Buckets, Bucket{
			Name:         bk,
			CreationDate: info.Info.Created,
		})
	}

	return lr
}

// SetBucketPolicy 设置 bucket policy
func (ms *MinioServer) SetBucketPolicy(bucket, policy string) bool {
	ms.Lock()
	defer ms.Unlock()

	var bi *BucketData
	var ok bool
	if bi, ok = ms.Buckets[bucket]; !ok {
		return false
	}

	bi.Info.Policy = policy
	return true
}

func (ms *MinioServer) GetBucketPolicy(bucket string) (string, bool) {
	ms.Lock()
	defer ms.Unlock()

	var bi *BucketData
	var ok bool
	var policy string
	if bi, ok = ms.Buckets[bucket]; !ok {
		return policy, false
	}

	policy = bi.Info.Policy
	return policy, true
}

// MakeBucket 创建bucket
func (ms *MinioServer) MakeBucket(bucket string) bool {
	ms.Lock()
	defer ms.Unlock()
	if _, ok := ms.Buckets[bucket]; ok {
		return false
	}
	ms.Buckets[bucket] = &BucketData{
		Info: BucketInfo{
			Created: time.Now(),
		},
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

type LocationResponse struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LocationConstraint" json:"-"`
	Location string   `xml:",chardata"`
}

func (lr LocationResponse) Encode() []byte {
	return encodeAny(lr)
}

type ListBucketsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListAllMyBucketsResult" json:"-"`
	Owner   Owner
	Buckets struct {
		Buckets []Bucket `xml:"Bucket"`
	}
}

type Bucket struct {
	Name         string
	CreationDate time.Time
}

type Owner struct {
	DisplayName string
	ID          string
}

func (lr ListBucketsResponse) Encode() []byte {
	return encodeAny(lr)
}
