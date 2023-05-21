package model

import (
	"bytes"
	"encoding/xml"
)

type LocationResponse struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LocationConstraint" json:"-"`
	Location string   `xml:",chardata"`
}

func (lr LocationResponse) Encode() []byte {
	var bytesBuffer bytes.Buffer
	bytesBuffer.WriteString(xml.Header)
	e := xml.NewEncoder(&bytesBuffer)
	err := e.Encode(lr)
	if err != nil {
		return nil
	}
	return bytesBuffer.Bytes()
}
