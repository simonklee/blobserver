// Copyright 2014 Simon Zimmermann. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package storagetest

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"

	"github.com/simonz05/blobserver"
	"github.com/simonz05/blobserver/blob"
)

type fakeStorage struct {
	blobs map[string]blob.Blob
}

func NewFakeStorage() blobserver.Storage {
	return &fakeStorage{
		blobs: make(map[string]blob.Blob),
	}
}

func (sto *fakeStorage) FetchStreaming(b blob.Ref) (file io.ReadCloser, size uint32, err error) {
	bb, ok := sto.blobs[b.String()]

	if !ok {
		return file, size, errors.New("Blob not found")
	}

	return bb.Open(), bb.Size(), err
}

//func NewBlob(ref Ref, size uint32, newReader func() io.ReadCloser) Blob {
func (sto *fakeStorage) ReceiveBlob(b blob.Ref, source io.Reader) (sb blob.SizedRef, err error) {
	buf := &bytes.Buffer{}
	size, err := io.Copy(buf, source)

	if err != nil {
		return sb, err
	}

	b = blob.Ref{Path: "bucket/" + b.String(), Ref: b.Ref}
	newBlob := blob.NewBlob(b, uint32(size), func() io.ReadCloser {
		return ioutil.NopCloser(bytes.NewReader(buf.Bytes()))
	})

	sto.blobs[b.String()] = newBlob
	return newBlob.SizedRef(), err
}

func (sto *fakeStorage) RemoveBlobs(blobs []blob.Ref) error {
	for _, b := range blobs {
		if _, ok := sto.blobs[b.String()]; !ok {
			return errors.New("Blob not found")
		}
		delete(sto.blobs, b.String())
	}
	return nil
}

func (sto *fakeStorage) StatBlobs(dest chan<- blob.SizedRef, blobs []blob.Ref) error {
	for _, ref := range blobs {
		b, ok := sto.blobs[ref.String()]
		if !ok {
			return errors.New("Blob not found")
		}
		dest <- b.SizedRef()
	}
	return nil
}
