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

// Package s3 registers the "s3" blobserver storage type, storing
// blobs in an Amazon Web Services' S3 storage bucket.

package s3

import (
	"fmt"
	"net/http"

	"github.com/simonz05/blobserver"
	"github.com/simonz05/blobserver/config"
	"github.com/simonz05/util/amazon/s3"
)

type s3Storage struct {
	s3Client *s3.Client
	bucket   string
	hostname string
	cdnUrl   string
}

func (s *s3Storage) String() string {
	return fmt.Sprintf("\"s3\" blob storage at host %q, bucket %q", s.hostname, s.bucket)
}

func (s *s3Storage) Config() *blobserver.Config {
	return &blobserver.Config{
		CDNUrl: s.cdnUrl,
		Name:   "s3",
	}
}

func newFromConfig(config *config.Config) (blobserver.Storage, error) {
	s3conf := config.S3
	hostname := s3conf.Hostname

	if hostname == "" {
		hostname = "s3.amazonaws.com"
	}

	client := &s3.Client{
		Auth: &s3.Auth{
			AccessKey:       s3conf.AccessKey,
			SecretAccessKey: s3conf.SecretAccessKey,
			Hostname:        hostname,
		},
		HTTPClient: http.DefaultClient,
		DefaultACL: s3conf.DefaultACL,
	}
	sto := &s3Storage{
		s3Client: client,
		bucket:   s3conf.Bucket,
		hostname: hostname,
		cdnUrl:   s3conf.CDNUrl,
	}
	return sto, nil
}

func init() {
	blobserver.RegisterStorageConstructor("s3", blobserver.StorageConstructor(newFromConfig))
}
