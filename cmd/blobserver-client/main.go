// Copyright 2014 Simon Zimmermann. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"

	"os"

	"github.com/simonz05/blobserver/client"
	"github.com/simonz05/util/log"
)

var (
	serverAddr = flag.String("addr", "http://localhost:6064/v1/api/blobserver", "server addr")
	outputType = flag.String("output", "json", "print result")
)

func main() {
	flag.Parse()
	log.Println("start uploader")

	if flag.NArg() == 0 {
		log.Fatal("Expected file argument missing")
	}

	c, err := client.New(*serverAddr)

	if err != nil {
		log.Fatal(err)
	}

	res, _ := c.MultiUploader(flag.Args())
	var enc client.ResourceEncoder

	switch *outputType {
	case "json":
		enc = client.NewJSONEncoder(os.Stdout)
	default:
		os.Exit(0)
	}

	err = enc.Encode(res)

	if err != nil {
		log.Fatal(err)
	}
}
