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

// Package storage implements storage backend

package blobserver

import (
	"io"
	"os"

	"github.com/simonz05/blobserver/blob"
)

// BlobReceiver is the interface for receiving
type BlobReceiver interface {
	// ReceiveBlob accepts a newly uploaded blob and writes it to
	// permanent storage.
	//
	// Implementations of BlobReceiver downstream of the HTTP
	// server can trust that the source isn't larger than
	// MaxBlobSize and that its digest matches the provided blob
	// ref. (If not, the read of the source will fail before EOF)
	ReceiveBlob(br blob.Ref, source io.Reader) (blob.SizedRef, error)
}

type BlobStatter interface {
	// Stat checks for the existence of blobs, writing their sizes
	// (if found back to the dest channel), and returning an error
	// or nil.  Stat() should NOT close the channel.
	// TODO(bradfitz): redefine this to close the channel? Or document
	// better what the synchronization rules are.
	StatBlobs(dest chan<- blob.SizedRef, blobs []blob.Ref) error
}

func StatBlob(bs BlobStatter, br blob.Ref) (sb blob.SizedRef, err error) {
	c := make(chan blob.SizedRef, 1)
	err = bs.StatBlobs(c, []blob.Ref{br})
	if err != nil {
		return
	}
	select {
	case sb = <-c:
	default:
		err = os.ErrNotExist
	}
	return
}

type StatReceiver interface {
	BlobReceiver
	BlobStatter
}

type BlobRemover interface {
	// RemoveBlobs removes 0 or more blobs.  Removal of
	// non-existent items isn't an error.  Returns failure if any
	// items existed but failed to be deleted.
	// ErrNotImplemented may be returned for storage types not implementing removal.
	RemoveBlobs(blobs []blob.Ref) error
}

// Storage is the interface that must be implemented by a blobserver
// storage type. (e.g. localdisk, s3, encrypt, shard, replica, remote)
type Storage interface {
	blob.Fetcher
	BlobReceiver
	BlobStatter
	BlobRemover
}

// Optional interface for storage implementations which can be asked
// to shut down cleanly. Regardless, all implementations should
// be able to survive crashes without data loss.
type ShutdownStorage interface {
	Storage
	io.Closer
}

//type BlobReceiveConfiger interface {
//	BlobReceiver
//	Configer
//}

type Config struct {
	CDNUrl string
	Name   string
}

type Configer interface {
	Config() *Config
}

type StorageConfiger interface {
	Storage
	Configer
}
