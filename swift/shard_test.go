// Copyright 2014 Simon Zimmermann. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package swift

import (
	"testing"
)

func TestShard(t *testing.T) {
	s := newSharder()
	tests := []struct {
		data string
		exp  string
	}{
		{
			data: string("\x00"),
			exp:  "3b8",
		},
	}

	for i, tt := range tests {
		shard := s.shard(tt.data)
		if shard != tt.exp {
			t.Fatalf("%d: exp %s got %s", i, tt.exp, shard)
		}
	}
}
