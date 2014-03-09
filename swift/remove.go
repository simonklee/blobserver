// Copyright 2014 Simon Zimmermann. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package swift

import (
	"github.com/simonz05/blobserver/blob"
	"github.com/simonz05/util/syncutil"
)

var removeGate = syncutil.NewGate(20) // arbitrary

func (sto *swiftStorage) RemoveBlobs(blobs []blob.Ref) error {
	var wg syncutil.Group

	for _, blob := range blobs {
		blob := blob
		removeGate.Start()
		wg.Go(func() error {
			defer removeGate.Done()
			return sto.conn.ObjectDelete(sto.container, blob.String())
		})
	}
	return wg.Err()
}
