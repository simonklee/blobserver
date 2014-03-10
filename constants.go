// Copyright 2014 Simon Zimmermann. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package blobserver

// MaxBlobSize is the max size of a single blob.
const MaxBlobSize = 128 << 20

// MaxInMemory is max size of a blob before we use a temporary disk file
const MaxInMemory = 8 << 20
