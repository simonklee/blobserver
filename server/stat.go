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

package server

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/simonz05/blobserver"
	"github.com/simonz05/blobserver/blob"
	"github.com/simonz05/blobserver/protocol"
	"github.com/simonz05/util/httputil"
	"github.com/simonz05/util/log"
)

const maxStatBlobs = 1000

func createBatchStatHandler(storage blobserver.BlobStatter) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		req.ParseForm()
		blobs, _ := req.Form["blob"]
		res, err := handleStat(req, storage, blobs)

		if err != nil {
			httputil.ServeJSONError(rw, err)
		} else {
			httputil.ReturnJSON(rw, res)
		}
	})
}

func createStatHandler(storage blobserver.BlobStatter) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		req.ParseForm()
		vars := mux.Vars(req)
		res, err := handleStat(req, storage, []string{vars["blobRef"]})

		if err != nil {
			httputil.ServeJSONError(rw, err)
		} else {
			httputil.ReturnJSON(rw, res)
		}
	})
}

func handleStat(req *http.Request, storage blobserver.BlobStatter, blobs []string) (interface{}, error) {
	res := new(protocol.StatResponse)
	needStat := map[blob.Ref]bool{}
	n := 0

	for _, value := range blobs {
		n++
		if value == "" {
			n--
			break
		}
		if n > maxStatBlobs {
			return nil, newRateLimitError(maxStatBlobs)
		}
		ref, ok := blob.Parse(value)
		if !ok {
			return nil, newHTTPError("Bogus blobref for value", http.StatusBadRequest)
		}
		needStat[ref] = true
	}

	toStat := make([]blob.Ref, 0, len(needStat))

	for br := range needStat {
		toStat = append(toStat, br)
	}

	log.Printf("Need to stat blob cnt: %d, got %d", len(needStat), len(blobs))
	blobch := make(chan blob.SizedInfoRef)
	errch := make(chan error, 1)

	go func() {
		err := storage.StatBlobs(blobch, toStat)
		close(blobch)
		errch <- err
	}()

	for sb := range blobch {
		res.Stat = append(res.Stat, protocol.RefInfo{
			Ref:  sb.Ref,
			Size: uint32(sb.Size),
			MD5:  sb.MD5,
		})
		delete(needStat, sb.Ref)
	}

	err := <-errch

	if err != nil {
		log.Errorf("Stat error: %v", err)
		return nil, newHTTPError("Server Error", http.StatusInternalServerError)
	}

	return res, nil
}
