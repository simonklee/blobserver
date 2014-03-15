/*
Copyright 2014 The Camlistore Authors
Modifications Copyright (c) 2014 Simon Zimmermann

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package blob defines types to refer to and retrieve low-level blobs.
package blob

import (
	"io"
)

// Blob represents a blob. Use the methods Size, SizedRef and
// Open to query and get data from Blob.
type Blob struct {
	ref       Ref
	size      uint32
	newReader func() io.ReadCloser
}

// NewBlob constructs a Blob from its Ref, size and a function that
// returns an io.ReadCloser from which the blob can be read. Any error
// in the function newReader when constructing the io.ReadCloser should
// be returned upon the first call to Read or Close.
func NewBlob(ref Ref, size uint32, newReader func() io.ReadCloser) Blob {
	return Blob{ref, size, newReader}
}

// Size returns the size of the blob (in bytes).
func (b Blob) Size() uint32 {
	return b.size
}

// SizedRef returns the SizedRef corresponding to the blob.
func (b Blob) SizedRef() SizedRef {
	return SizedRef{b.ref, b.size}
}

// Open returns an io.ReadCloser that can be used to read the blob
// data. The caller must close the io.ReadCloser when finished.
func (b Blob) Open() io.ReadCloser {
	return b.newReader()
}
