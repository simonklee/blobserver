/*
Copyright 2013 The Camlistore Authors
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

// Package storagetest tests blobserver.Storage implementations
package storagetest

import (
	"crypto/sha1"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/simonz05/blobserver"
	"github.com/simonz05/blobserver/blob"
)

func Test(t *testing.T, fn func(*testing.T) (sto blobserver.Storage, cleanup func())) {
	sto, cleanup := fn(t)
	defer func() {
		if t.Failed() {
			t.Logf("test %T FAILED, skipping cleanup!", sto)
		} else {
			cleanup()
		}
	}()
	t.Logf("Testing blobserver storage %T", sto)

	var blobs []*Blob
	var blobRefs []blob.Ref
	var blobSizedRefs []blob.SizedRef

	contents := []string{"foo", "quux", "asdf", "qwerty", "0123456789"}
	if !testing.Short() {
		for i := 0; i < 95; i++ {
			contents = append(contents, "foo-"+strconv.Itoa(i))
		}
	}
	t.Logf("Testing receive")
	for _, x := range contents {
		b1 := &Blob{x}
		b1s, err := sto.ReceiveBlob(b1.BlobRef(), b1.Reader())
		if err != nil {
			t.Fatalf("ReceiveBlob of %s: %v", b1, err)
		}
		if b1s != b1.SizedRef() {
			t.Fatal("Received %v; want %v", b1s, b1.SizedRef())
		}
		blobs = append(blobs, b1)
		blobRefs = append(blobRefs, b1.BlobRef())
		blobSizedRefs = append(blobSizedRefs, b1.SizedRef())
	}
	b1 := blobs[0]

	// finish here if you want to examine the test directory
	//t.Fatalf("FINISH")

	t.Logf("Testing FetchStreaming")
	for i, b2 := range blobs {
		rc, size, err := sto.FetchStreaming(b2.BlobRef())
		if err != nil {
			t.Fatalf("error fetching %d. %s: %v", i, b2, err)
		}
		defer rc.Close()
		testSizedBlob(t, rc, b2.BlobRef(), int64(size))
	}

	if fetcher, ok := sto.(fetcher); ok {
		rsc, size, err := fetcher.Fetch(b1.BlobRef())
		if err != nil {
			t.Fatalf("error fetching %s: %v", b1, err)
		}
		defer rsc.Close()
		n, err := rsc.Seek(0, 0)
		if err != nil {
			t.Fatalf("error seeking in %s: %v", rsc, err)
		}
		if n != 0 {
			t.Fatalf("after seeking to 0, we are at %d!", n)
		}
		testSizedBlob(t, rsc, b1.BlobRef(), size)
	}

	t.Logf("Testing Stat")
	dest := make(chan blob.SizedRef)
	go func() {
		if err := sto.StatBlobs(dest, blobRefs); err != nil {
			t.Fatalf("error stating blobs %s: %v", blobRefs, err)
		}
	}()
	testStat(t, dest, blobSizedRefs)

	t.Logf("Testing Remove")
	if err := sto.RemoveBlobs(blobRefs); err != nil {
		if strings.Contains(err.Error(), "not implemented") {
			t.Logf("RemoveBlob %s: %v", b1, err)
		} else {
			t.Fatalf("RemoveBlob %s: %v", b1, err)
		}
	}
}

type fetcher interface {
	Fetch(blob blob.Ref) (blobserver.ReadSeekCloser, int64, error)
}

func testSizedBlob(t *testing.T, r io.Reader, b1 blob.Ref, size int64) {
	h := b1.Hash()
	n, err := io.Copy(h, r)
	if err != nil {
		t.Fatalf("error reading from %s: %v", r, err)
	}
	if n != size {
		t.Fatalf("read %d bytes from %s, metadata said %d!", n, size)
	}
	b2 := blob.RefFromHash(h)
	if b2 != b1 {
		t.Fatalf("content mismatch (awaited %s, got %s)", b1, b2)
	}
}

func testStat(t *testing.T, enum <-chan blob.SizedRef, want []blob.SizedRef) {
	// blobs may arrive in ANY order
	m := make(map[string]int, len(want))
	for i, sb := range want {
		m[sb.Ref.String()] = i
	}

	i := 0
	for sb := range enum {
		if !sb.Valid() {
			break
		}
		wanted := want[m[sb.Ref.String()]]
		if wanted.Size != sb.Size {
			t.Fatalf("received blob size is %d, wanted %d for &%d", sb.Size, wanted.Size, i)
		}
		if wanted.Ref != sb.Ref {
			t.Fatalf("received blob ref mismatch &%d: wanted %s, got %s", i, sb.Ref, wanted.Ref)
		}
		i++
		if i >= len(want) {
			break
		}
	}
}

// Blob is a utility class for unit tests.
type Blob struct {
	Contents string // the contents of the blob
}

func (tb *Blob) BlobRef() blob.Ref {
	h := sha1.New()
	h.Write([]byte(tb.Contents))
	return blob.RefFromHash(h)
}

func (tb *Blob) SizedRef() blob.SizedRef {
	return blob.SizedRef{tb.BlobRef(), uint32(len(tb.Contents))}
}

func (tb *Blob) BlobRefSlice() []blob.Ref {
	return []blob.Ref{tb.BlobRef()}
}

func (tb *Blob) Size() int64 {
	return int64(len(tb.Contents))
}

func (tb *Blob) Reader() io.Reader {
	return strings.NewReader(tb.Contents)
}

func (tb *Blob) AssertMatches(t *testing.T, sb blob.SizedRef) {
	if int64(sb.Size) != tb.Size() {
		t.Fatalf("Got size %d; expected %d", sb.Size, tb.Size())
	}
	if sb.Ref != tb.BlobRef() {
		t.Fatalf("Got blob %q; expected %q", sb.Ref.String(), tb.BlobRef())
	}
}

func (tb *Blob) MustUpload(t *testing.T, ds blobserver.BlobReceiver) {
	sb, err := ds.ReceiveBlob(tb.BlobRef(), tb.Reader())
	if err != nil {
		t.Fatalf("failed to upload blob %v (%q): %v", tb.BlobRef(), tb.Contents, err)
	}
	tb.AssertMatches(t, sb) // TODO: better error reporting
}
