package config

import (
	"github.com/BurntSushi/toml"
)

type Config struct {
	Listen   string
	S3		 *S3Config
}

type S3Config struct {
	Hostname string
	AccessKey    string `toml:"aws_access_key"`
	SecretAccessKey string `toml:"aws_secret_access_key"`
	Bucket string
}

func (c *Config) StorageType() string {
	if c.S3 != nil {
		return "s3"
	}
	return ""
}

func FromFile(filename string) (config *Config, err error) {
	_, err = toml.DecodeFile(filename, config)
	return 
}
