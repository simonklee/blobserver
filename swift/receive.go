// Copyright 2014 Simon Zimmermann. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package swift

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"os"
	//"time"

	"github.com/ncw/swift"
	"github.com/simonz05/blobserver/blob"
)

const maxInMemorySlurp = 8 << 20 // 8MB.

// swiftSlurper slurps up a blob to memory (or spilling to disk if
// over maxInMemorySlurp) to verify its digest (and also gets its MD5
// for Amazon's Content-MD5 header, even if the original blobref
// is e.g. sha1-xxxx)
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

func (as *swiftSlurper) Read(p []byte) (n int, err error) {
	if !as.reading {
		as.reading = true
		if as.file != nil {
			as.file.Seek(0, 0)
		}
	}
	if as.file != nil {
		return as.file.Read(p)
	}
	if as.r == nil {
		as.r = bytes.NewReader(as.buf.Bytes())
	}
	return as.r.Read(p)
}

func (as *swiftSlurper) Seek(offset int64, whence int) (int64, error) {
	if as.file != nil {
		return as.file.Seek(offset, whence)
	}
	if as.r != nil {
		return as.r.Seek(offset, whence)
	}
	return offset, nil
}

func (as *swiftSlurper) Write(p []byte) (n int, err error) {
	if as.reading {
		panic("write after read")
	}
	as.md5.Write(p)
	if as.file != nil {
		n, err = as.file.Write(p)
		return
	}

	if as.buf.Len()+len(p) > maxInMemorySlurp {
		as.file, err = ioutil.TempFile("", as.blob.String())
		if err != nil {
			return
		}
		_, err = io.Copy(as.file, as.buf)
		if err != nil {
			return
		}
		as.buf = nil
		n, err = as.file.Write(p)
		return
	}

	return as.buf.Write(p)
}

func (as *swiftSlurper) Cleanup() {
	if as.file != nil {
		os.Remove(as.file.Name())
	}
}

func (sto *swiftStorage) ReceiveBlob(b blob.Ref, source io.Reader) (sr blob.SizedRef, err error) {
	slurper := newSwiftSlurper(b)
	defer slurper.Cleanup()

	size, err := io.Copy(slurper, source)

	if err != nil {
		return sr, err
	}

	hash := fmt.Sprintf("%x", slurper.md5.Sum(nil))
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
	return blob.SizedRef{Ref: b, Size: uint32(size)}, nil
}
