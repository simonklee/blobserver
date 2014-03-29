// Copyright 2014 Simon Zimmermann. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package swift

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"hash"
	"io"
	"io/ioutil"
	"os"

	"github.com/simonz05/blobserver"
	"github.com/simonz05/blobserver/blob"
	"github.com/simonz05/blobserver/third_party/github.com/ncw/swift"
	"github.com/simonz05/blobserver/third_party/github.com/simonz05/util/log"
)

// swiftSlurper slurps up a blob to memory (or spilling to disk if
// over MaxInMemory) to verify its digest.
type swiftSlurper struct {
	blob    blob.Ref // only used for tempfile's prefix
	buf     *bytes.Buffer
	r       *bytes.Reader
	md5     hash.Hash
	file    *os.File // nil until allocated
	reading bool     // transitions at most once from false -> true
}

func newSwiftSlurper(blob blob.Ref) *swiftSlurper {
	return &swiftSlurper{
		blob: blob,
		buf:  new(bytes.Buffer),
		md5:  md5.New(),
	}
}

func (ss *swiftSlurper) Read(p []byte) (n int, err error) {
	if !ss.reading {
		ss.reading = true
		if ss.file != nil {
			ss.file.Seek(0, 0)
		}
	}
	if ss.file != nil {
		return ss.file.Read(p)
	}
	if ss.r == nil {
		ss.r = bytes.NewReader(ss.buf.Bytes())
	}
	return ss.r.Read(p)
}

func (ss *swiftSlurper) Seek(offset int64, whence int) (int64, error) {
	if ss.file != nil {
		return ss.file.Seek(offset, whence)
	}
	if ss.r != nil {
		return ss.r.Seek(offset, whence)
	}
	return offset, nil
}

func (ss *swiftSlurper) Write(p []byte) (n int, err error) {
	if ss.reading {
		panic("write after read")
	}
	ss.md5.Write(p)
	if ss.file != nil {
		n, err = ss.file.Write(p)
		return
	}

	if ss.buf.Len()+len(p) > blobserver.MaxInMemory {
		ss.file, err = ioutil.TempFile("", ss.blob.String())
		if err != nil {
			return
		}
		_, err = io.Copy(ss.file, ss.buf)
		if err != nil {
			return
		}
		ss.buf = nil
		n, err = ss.file.Write(p)
		return
	}

	return ss.buf.Write(p)
}

func (ss *swiftSlurper) Cleanup() {
	if ss.file != nil {
		os.Remove(ss.file.Name())
	}
}

func (sto *swiftStorage) ReceiveBlob(b blob.Ref, source io.Reader) (sr blob.SizedRef, err error) {
	slurper := newSwiftSlurper(b)
	defer slurper.Cleanup()

	size, err := io.Copy(slurper, source)

	if err != nil {
		return sr, err
	}

	hash := hex.EncodeToString(slurper.md5.Sum(nil))
	retries := 1
retry:
	_, err = sto.conn.ObjectPut(sto.container(b), b.String(), slurper, false, hash, "", nil)

	if err != nil {
		// assume both of these mean container not found in this context
		if (err == swift.ObjectNotFound || err == swift.ContainerNotFound) && retries > 0 {
			retries--
			slurper.Seek(0, 0)
			h := make(swift.Headers)
			h["X-Container-Read"] = sto.containerReadACL
			err = sto.conn.ContainerCreate(sto.container(b), h)
			if err == nil {
				goto retry
			}
		}
		return sr, err
	}
	ref := sto.createPathRef(b)
	ref.SetHash(slurper.md5)
	log.Println("Create: ", ref)
	return blob.SizedRef{Ref: ref, Size: uint32(size)}, nil
}
