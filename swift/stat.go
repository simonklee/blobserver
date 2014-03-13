// Copyright 2014 Simon Zimmermann. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package swift

import (
	"fmt"
	"os"

	"github.com/simonz05/blobserver/blob"
	"github.com/simonz05/util/log"
	"github.com/simonz05/util/syncutil"
)

var statGate = syncutil.NewGate(20) // arbitrary

func (sto *swiftStorage) StatBlobs(dest chan<- blob.SizedRef, blobs []blob.Ref) error {
	var wg syncutil.Group

	for _, br := range blobs {
		br := br
		statGate.Start()
		wg.Go(func() error {
			defer statGate.Done()
			ref, cont := sto.refContainer(br)
			log.Println("Stat:", cont, ref)
			info, _, err := sto.conn.Object(cont, ref)

			if err == nil {
				dest <- blob.SizedRef{Ref: br, Size: uint32(info.Bytes)}
				return nil
			}
			if err == os.ErrNotExist {
				return nil
			}
			return fmt.Errorf("error statting %v: %v", br, err)
		})
	}
	return wg.Err()
}
