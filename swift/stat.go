// Copyright 2014 Simon Zimmermann. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package swift

import (
	"fmt"

	"github.com/ncw/swift"
	"github.com/simonz05/blobserver/blob"
	"github.com/simonz05/util/log"
	"github.com/simonz05/util/syncutil"
)

var statGate = syncutil.NewGate(20) // arbitrary

func (sto *swiftStorage) StatBlobs(dest chan<- blob.SizedInfoRef, blobs []blob.Ref) error {
	var wg syncutil.Group

	for _, br := range blobs {
		br := sto.createPathRef(br)
		statGate.Start()
		wg.Go(func() error {
			defer statGate.Done()
			ref, cont := sto.refContainer(br)
			log.Println("REF:", ref, cont)
			info, _, err := sto.conn.Object(cont, ref)
			log.Println("Stat:", info, err, ref, br.Path)

			if err == nil {
				dest <- blob.SizedInfoRef{
					Ref:  br,
					Size: uint32(info.Bytes),
					MD5:  info.Hash,
				}
				return nil
			}
			if err == swift.ObjectNotFound {
				return nil
			}
			return fmt.Errorf("error statting %v: %v", br, err)
		})
	}
	return wg.Err()
}
