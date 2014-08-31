// Copyright 2014 Simon Zimmermann. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/tideland/goas/v2/monitoring"
	"github.com/simonz05/blobserver"
	"github.com/simonz05/blobserver/config"
	_ "github.com/simonz05/blobserver/s3"
	"github.com/simonz05/blobserver/server"
	_ "github.com/simonz05/blobserver/swift"
	"github.com/simonz05/util/log"
)

var (
	help           = flag.Bool("h", false, "show help text")
	laddr          = flag.String("http", ":6064", "set bind address for the HTTP server")
	version        = flag.Bool("version", false, "show version number and exit")
	configFilename = flag.String("config", "config.toml", "config file path")
	cpuprofile     = flag.String("debug.cpuprofile", "", "write cpu profile to file")
)

var Version = "0.1.0"

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS]\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "\nOptions:\n")
	flag.PrintDefaults()
}

func main() {
	flag.Usage = usage
	flag.Parse()
	log.Println("start blobserver service …")

	if *version {
		fmt.Fprintln(os.Stdout, Version)
		return
	}

	if *help {
		flag.Usage()
		os.Exit(1)
	}

	conf, err := config.ReadFile(*configFilename)

	if err != nil {
		log.Fatal(err)
	}

	if conf.Listen == "" && *laddr == "" {
		log.Fatal("Listen address required")
	} else if conf.Listen == "" {
		conf.Listen = *laddr
	}

	runtime.GOMAXPROCS(runtime.NumCPU())

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	storage, err := blobserver.CreateStorage(conf)

	if err != nil {
		log.Fatalf("error instantiating storage for type %s: %v",
			conf.StorageType(), err)
	}

	log.Printf("Using `%s` storage", conf.StorageType())
	err = server.ListenAndServe(*laddr, storage)

	if err != nil {
		log.Errorln(err)
	}

	monitoring.MeasuringPointsPrintAll()
}
