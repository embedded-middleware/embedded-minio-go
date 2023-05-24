package model

import (
	"bytes"
	"encoding/xml"
)

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

type LocationResponse struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LocationConstraint" json:"-"`
	Location string   `xml:",chardata"`
}

func (lr LocationResponse) Encode() []byte {
	return encodeAny(lr)
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
