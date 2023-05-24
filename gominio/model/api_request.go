package model

import (
	"encoding/xml"
	"io"
)

func decodeAny(r io.Reader, v any) {
	var err error
	err = xml.NewDecoder(r).Decode(v)
	if err != nil {
		return
	}
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
