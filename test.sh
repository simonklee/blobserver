#!/bin/bash

swift_config="`pwd`/swift/config.toml"
s3_config="`pwd`/s3/config.toml"

if [ -f ${swift_config} ]; then
    export BLOBSERVER_SWIFT_TEST_CONFIG="${swift_config}"
fi

if [ -f ${swift_config} ]; then
    export BLOBSERVER_S3_TEST_CONFIG="${s3_config}"
fi

go test ./... $@
