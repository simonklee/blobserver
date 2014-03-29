/*
Copyright 2013 Google Inc.
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
package blob

import (
	"fmt"
	"hash"
	"path/filepath"

	"github.com/simonz05/blobserver/third_party/github.com/nu7hatch/gouuid"
)

// Ref is a reference to a blob.
type Ref struct {
	Path string `json:"Path"`
	h    hash.Hash
}

func NewRef(name string) Ref {
	id, _ := uuid.NewV4()
	ext := filepath.Ext(name)

	if ext == "" {
		ext = ".bin"
	}

	buf := getBuf(len(id) + len(ext))[:0]
	defer putBuf(buf)

	buf = append(buf, id.String()...)
	buf = append(buf, ext...)
	return Ref{Path: string(buf)}
}

func (r Ref) String() string {
	return r.Path
}

func (r Ref) Sum32() uint32 {
	var v uint32
	for _, b := range r.Path[:4] {
		v = v<<8 | uint32(b)
	}
	return v
}

func (r *Ref) SetHash(h hash.Hash) {
	r.h = h
}

func (r Ref) Hash() hash.Hash {
	return r.h
}

var null = []byte(`null`)

func Parse(ref string) (r Ref, ok bool) {
	if len(ref) == 0 {
		return r, false
	}
	r.Path = ref
	return r, true
}

// SizedRef is like a Ref but includes a size.
type SizedRef struct {
	Ref
	Size uint32
}

func (sr SizedRef) String() string {
	return fmt.Sprintf("[%s; %d bytes]", sr.Ref.String(), sr.Size)
}

func NewSizedRef(name string) SizedRef {
	return SizedRef{Ref: NewRef(name)}
}

// SizedRef is like a Ref but includes a size.
type SizedInfoRef struct {
	Ref
	Size uint32
}

var bufPool = make(chan []byte, 20)

func getBuf(size int) []byte {
	for {
		select {
		case b := <-bufPool:
			if cap(b) >= size {
				return b[:size]
			}
		default:
			return make([]byte, size)
		}
	}
}

func putBuf(b []byte) {
	select {
	case bufPool <- b:
	default:
	}
}
