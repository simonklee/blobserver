// Copyright 2014 Simon Zimmermann. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

// Package swift registers the "swift" blobserver storage type, storing
// blobs in an OpenStack Swift storage.

package swift

import (
	"fmt"
	"strings"

	"github.com/ncw/swift"
	"github.com/simonz05/blobserver"
	"github.com/simonz05/blobserver/blob"
	"github.com/simonz05/blobserver/config"
)

var shards sharder

type swiftStorage struct {
	conn             *swift.Connection
	containerName    string
	shard            bool
	containerReadACL string
	cdnUrl           string
}

func (s *swiftStorage) String() string {
	return fmt.Sprintf("\"swift\" blob storage at host %q, container %q", s.conn.AuthUrl, s.container)
}

func (s *swiftStorage) Config() *blobserver.Config {
	return &blobserver.Config{
		CDNUrl: s.cdnUrl,
		Name:   "swift",
	}
}

func (s *swiftStorage) container(b blob.Ref) string {
	if !s.shard {
		return s.containerName
	}

	ref := b.String()
	idx := strings.Index(ref, "/")

	if idx > 0 {
		return ref[:idx]
	}

	return fmt.Sprintf("%s-%s", s.containerName, shards.shard(ref[:]))
}

func (s *swiftStorage) refContainer(b blob.Ref) (name string, container string) {
	ref := b.String()
	idx := strings.Index(ref, "/")

	if idx > 0 && len(ref) > idx+1 {
		return ref[idx+1:], ref[:idx]
	}

	return b.String(), s.container(b)
}

func (sto *swiftStorage) createContainer(name string) error {
	header := make(swift.Headers)
	header["X-Container-Read"] = sto.containerReadACL
	return sto.conn.ContainerCreate(name, header)
}

func (s *swiftStorage) createPathRef(b blob.Ref) blob.Ref {
	name, cont := s.refContainer(b)
	return blob.Ref{Path: cont + "/" + name}
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
		cdnUrl:           swiftConf.CDNUrl,
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

func init() {
	shards = newSharder()
	blobserver.RegisterStorageConstructor("swift", blobserver.StorageConstructor(newFromConfig))
}
