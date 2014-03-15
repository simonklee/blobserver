/*
Copyright 2011 Google Inc.
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
	"io"
)

// Fetcher is the minimal interface for retrieving a blob from storage.
// The full storage interface is blobserver.Storage.
type Fetcher interface {
	// Fetch returns a blob.  If the blob is not found then
	// os.ErrNotExist should be returned for the error (not a wrapped
	// error with a ErrNotExist inside)
	//
	// The caller should close blob.
	Fetch(Ref) (blob io.ReadCloser, size uint32, err error)
}
