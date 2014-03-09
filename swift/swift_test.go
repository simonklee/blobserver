// Copyright 2014 Simon Zimmermann. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package swift

import (
	"os"
	"testing"

	"github.com/simonz05/blobserver"
	"github.com/simonz05/blobserver/config"
	"github.com/simonz05/blobserver/storagetest"
)

func TestSwift(t *testing.T) {
	configFile := os.Getenv("BLOBSERVER_SWIFT_TEST_CONFIG")
	if configFile == "" {
		t.Skip("Skipping manual test. To enable, set the environment variable BLOBSERVER_SWIFT_TEST_CONFIG to the path of a JSON configuration for the s3 storage type.")
	}
	conf, err := config.ReadFile(configFile)
	if err != nil {
		t.Fatalf("Error reading swift configuration file %s: %v", configFile, err)
	}
	storagetest.Test(t, func(t *testing.T) (sto blobserver.Storage, cleanup func()) {
		sto, err := newFromConfig(conf)
		if err != nil {
			t.Fatalf("newFromConfig error: %v", err)
		}
		return sto, func() {}
	})
}
