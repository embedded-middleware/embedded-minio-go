package gominio

import (
	"encoding/xml"
	"errors"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7/pkg/tags"
	"io"
	"sort"
	"time"
)

// PutObjectTagging put object tagging
func (ms *MinioServer) PutObjectTagging(bucket, object string, tags *tags.Tags) error {
	ms.Lock()
	defer ms.Unlock()

	oi, err := ms.getObjectInfo(bucket, object)
	if err != nil {
		return err
	}

	oi.Tags = tags

	return nil
}

// RemoveObjectTagging remove object tagging
func (ms *MinioServer) RemoveObjectTagging(bucket, object string) error {
	ms.Lock()
	defer ms.Unlock()

	oi, err := ms.getObjectInfo(bucket, object)
	if err != nil {
		return err
	}

	tag, err := tags.MapToObjectTags(map[string]string{})
	if err != nil {
		return err
	}
	oi.Tags = tag

	return nil
}

// PutObject put object
func (ms *MinioServer) PutObject(bucket, object, etag string, content []byte) error {
	ms.Lock()
	defer ms.Unlock()

	var ok bool
	var bd *BucketData
	if bd, ok = ms.Buckets[bucket]; !ok {
		return errors.New("bucket not exists")
	}

	tag, err := tags.MapToObjectTags(map[string]string{})
	if err != nil {
		return err
	}

	bd.Objects[object] = &ObjectInfo{
		Name:         object,
		Size:         uint64(len(content)),
		Etag:         etag,
		Data:         content,
		Tags:         tag,
		LastModified: time.Now(),
	}
	return nil
}

// PutObjectPart put object part
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
		tag, err := tags.MapToObjectTags(map[string]string{})
		if err != nil {
			return err
		}
		oi = &ObjectInfo{
			Name:        object,
			Tags:        tag,
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

// CompleteObjectPart merge object parts
func (ms *MinioServer) CompleteObjectPart(bucket, object, id string, parts *CompleteMultiPart) (string, error) {
	ms.Lock()
	defer ms.Unlock()

	var etag = GetUid()
	var oi *ObjectInfo
	var err error

	oi, err = ms.getObjectInfo(bucket, object)
	if err != nil {
		return etag, err
	}

	if oi.UploadId != id {
		return etag, errors.New("object not exists")
	}

	sort.Sort(parts)
	for _, v := range parts.Parts {
		var part Multipart
		var ok bool
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

// DeleteObject delete object
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

func (ms *MinioServer) getObjectInfo(bucket, object string) (*ObjectInfo, error) {
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

// GetObject get object
func (ms *MinioServer) GetObject(bucket, object string) (*ObjectInfo, error) {
	ms.RLock()
	defer ms.RUnlock()

	return ms.getObjectInfo(bucket, object)
}

// GetUid get uid as etag
func GetUid() string {
	var id string
	uid, err := uuid.NewUUID()
	if err != nil {
		return id
	}
	id = uid.String()
	return id
}

type CompletePart struct {
	PartNumber int
	ETag       string
}

type CompleteMultiPart struct {
	Parts []CompletePart `xml:"Part"`
}

func (cmp CompleteMultiPart) Len() int      { return len(cmp.Parts) }
func (cmp CompleteMultiPart) Swap(i, j int) { cmp.Parts[i], cmp.Parts[j] = cmp.Parts[j], cmp.Parts[i] }
func (cmp CompleteMultiPart) Less(i, j int) bool {
	return cmp.Parts[i].PartNumber < cmp.Parts[j].PartNumber
}

func (cmp *CompleteMultiPart) Decode(r io.Reader) {
	decodeAny(r, cmp)
}

type InitiateMultipartUploadResult struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ InitiateMultipartUploadResult" json:"-"`

	Bucket   string
	Key      string
	UploadID string `xml:"UploadId"`
}

func (ir InitiateMultipartUploadResult) Encode() []byte {
	return encodeAny(ir)
}

type CompleteMultipartUploadResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CompleteMultipartUploadResult" json:"-"`

	Location string
	Bucket   string
	Key      string
	ETag     string
}

func (cr CompleteMultipartUploadResponse) Encode() []byte {
	return encodeAny(cr)
}
