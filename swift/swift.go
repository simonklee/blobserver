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
	"github.com/simonz05/blobserver/blob"
	"github.com/simonz05/blobserver/config"
)

type swiftStorage struct {
	conn             *swift.Connection
	containerName    string
	shard            bool
	containerReadACL string
	config           *config.SwiftConfig
}

func (s *swiftStorage) String() string {
	return fmt.Sprintf("\"swift\" blob storage at host %q, container %q", s.conn.AuthUrl, s.container)
}

func (s *swiftStorage) container(b blob.Ref) string {
	if !s.shard {
		return s.containerName
	}

	return fmt.Sprintf("%s-%s", s.containerName, shards[b.Sum32()%uint32(shardCount)])
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
		conn:             conn,
		shard:            swiftConf.Shard,
		containerName:    swiftConf.Container,
		containerReadACL: ".r:*,.rlistings",
		config:           swiftConf,
	}

	if swiftConf.ContainerReadACL != "" {
		sto.containerReadACL = swiftConf.ContainerReadACL
	}

	err := sto.conn.Authenticate()
	if err != nil {
		return nil, err
	}
	return sto, nil
}

const shardCount = 2 // 8<<5

var shards [shardCount]string

func init() {
	for i := range shards {
		shards[i] = fmt.Sprintf("%0.2X", i)
	}
	blobserver.RegisterStorageConstructor("swift", blobserver.StorageConstructor(newFromConfig))
}
