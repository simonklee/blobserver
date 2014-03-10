// Copyright 2014 Simon Zimmermann. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package config

import (
	"github.com/BurntSushi/toml"
)

type Config struct {
	Listen string
	S3     *S3Config
	Swift  *SwiftConfig
}

type S3Config struct {
	Hostname        string // Optional. Default s3.amazonaws.com
	AccessKey       string `toml:"access_key"`
	SecretAccessKey string `toml:"secret_access_key"`
	Bucket          string
	DefaultACL      string `toml:"default_acl"` // optional. Default private. public-read
}

type SwiftConfig struct {
	APIUser          string `toml:"api_user"`
	APIKey           string `toml:"api_key"`
	AuthURL          string `toml:"auth_url"`
	Tenant           string `toml:"tenant"`
	TenantID         string `toml:"tenant_id"`
	Region           string `toml:"region"`
	Container        string `toml:"container"`
	ContainerReadACL string `toml:"container_read_acl"`
}

func (c *Config) StorageType() string {
	if c.S3 != nil {
		return "s3"
	}
	if c.Swift != nil {
		return "swift"
	}
	return ""
}

func ReadFile(filename string) (*Config, error) {
	config := new(Config)
	_, err := toml.DecodeFile(filename, config)
	return config, err
}
