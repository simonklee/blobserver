// Copyright 2014 Simon Zimmermann. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"strings"

	"os"

	"github.com/simonz05/blobserver/client"
	"github.com/simonz05/util/log"
)

var (
	help       = flag.Bool("h", false, "show help text")
	serverAddr = flag.String("addr", "http://localhost:6064/v1/api/blobserver", "server addr")
	staticDir  = flag.String("static", ".", "static dir for referenced url()")
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS] file ...\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "\nTransform CSS url path to remote URL path.\n")
	fmt.Fprintf(os.Stderr, "\nOptions:\n")
	flag.PrintDefaults()
}

func main() {
	flag.Usage = usage
	flag.Parse()
	log.Println("start uploader")

	if *help {
		flag.Usage()
		os.Exit(1)
	}

	ctx, err := newCtx(*serverAddr, flag.Args())

	if err != nil {
		log.Fatal(err)
	}

	ctx.search()
	err = ctx.upload()

	if err != nil {
		log.Fatal(err)
	}

	ctx.replace()
	ctx.WriteTo(os.Stdout)
}

type match struct {
	file    string
	cssPath string
	osPath  string
	URL     string
}

type matches []*match

func (f matches) osPaths() []string {
	res := make([]string, len(f))

	for i, r := range f {
		res[i] = r.osPath
	}

	return res
}

func (fs matches) updateURLs(res client.Resources) error {
	for _, f := range fs {
		found := false
		for _, r := range res {
			if f.osPath == r.Path {
				f.URL = r.URL
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("Not found %v", f)
		}
	}
	return nil
}

type context struct {
	files   map[string]string
	matches matches
	client  *client.Client
}

func newCtx(srvAddr string, paths []string) (*context, error) {
	c, err := client.New(srvAddr)
	if err != nil {
		return nil, err
	}
	ctx := &context{
		files:  make(map[string]string),
		client: c,
	}

	if len(paths) > 0 {
		for _, path := range paths {
			if _, ok := ctx.files[path]; ok {
				continue
			}

			buf, err := ioutil.ReadFile(path)

			if err != nil {
				return nil, err
			}

			ctx.files[path] = string(buf)
		}
	} else {
		buf, err := ioutil.ReadAll(os.Stdin)

		if err != nil {
			return nil, err
		}

		ctx.files["stdin"] = string(buf)
	}

	return ctx, nil
}

var (
	urlRe = regexp.MustCompile(`url\([[:space:]"']?(?P<path>[\/\._\-&=\?[:alnum:]]+)[[:space:]"']?\)`)
)

func (ctx *context) search() matches {
	exists := make(map[string]bool)
	result := make([]*match, 0)

	for file, str := range ctx.files {
		matches := urlRe.FindAllStringSubmatch(str, -1)

		if len(matches) == 0 {
			continue
		}

		for _, m := range matches {
			cssPath := m[1]
			if _, ok := exists[cssPath]; !ok {
				exists[cssPath] = true
				result = append(result, &match{
					file:    file,
					cssPath: cssPath,
					osPath:  filepath.Join(*staticDir, cssPath),
				})
			}
		}
	}

	ctx.matches = result
	return result
}

func (ctx *context) replace() {
	for k, str := range ctx.files {
		for _, m := range ctx.matches {
			str = strings.Replace(str, m.cssPath, m.URL, -1)
		}
		ctx.files[k] = str
	}
}

func (ctx *context) upload() error {
	if len(ctx.matches) == 0 {
		return nil
	}

	res, err := ctx.client.MultiUploader(ctx.matches.osPaths())

	if err != nil {
		return err
	}

	return ctx.matches.updateURLs(res)
}

func (ctx *context) WriteTo(w io.Writer) (n int64, err error) {
	for _, str := range ctx.files {
		nn, err := w.Write([]byte(str))

		if err != nil {
			return n, err
		}

		n += int64(nn)
	}
	return n, nil
}
