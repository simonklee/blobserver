// Copyright 2014 Simon Zimmermann. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

// Package swift registers the "swift" blobserver storage type, storing
// blobs in an OpenStack Swift storage.

package swift

import (
	"fmt"

	"github.com/ncw/swift"
	"github.com/simonz05/blobserver"
	"github.com/simonz05/blobserver/config"
)

type swiftStorage struct {
	conn      *swift.Connection
	container string
}

func (s *swiftStorage) String() string {
	return fmt.Sprintf("\"swift\" blob storage at host %q, container %q", s.conn.AuthUrl, s.container)
}

func newFromConfig(config *config.Config) (blobserver.Storage, error) {
	swiftConf := config.Swift

	conn := &swift.Connection{
		UserName: swiftConf.APIUser,
		ApiKey:   swiftConf.APIKey,
		AuthUrl:  swiftConf.AuthURL,
		Region:   swiftConf.Region,
		Tenant:   swiftConf.Tenant,
		//TenantId: swiftConf.TenantID,
	}
	sto := &swiftStorage{
		conn:      conn,
		container: swiftConf.Container,
	}

	err := sto.conn.Authenticate()
	if err != nil {
		return nil, err
	}
	return sto, nil
}

func init() {
	blobserver.RegisterStorageConstructor("swift", blobserver.StorageConstructor(newFromConfig))
}
