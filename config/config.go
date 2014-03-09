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
}

type S3Config struct {
	Hostname        string // Optional. Default s3.amazonaws.com
	AccessKey       string `toml:"aws_access_key"`
	SecretAccessKey string `toml:"aws_secret_access_key"`
	Bucket          string
}

func (c *Config) StorageType() string {
	if c.S3 != nil {
		return "s3"
	}
	return ""
}

func ReadFile(filename string) (*Config, error) {
	config := new(Config)
	_, err := toml.DecodeFile(filename, config)
	return config, err
}
