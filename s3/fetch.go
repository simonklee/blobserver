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

package s3

import (
	"io"

	"github.com/simonz05/blobserver/blob"
)

func (sto *s3Storage) Fetch(blob blob.Ref) (file io.ReadCloser, size uint32, err error) {
	file, sz, err := sto.s3Client.Get(sto.bucket, blob.String())
	return file, uint32(sz), err
}
