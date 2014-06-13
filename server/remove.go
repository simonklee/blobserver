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
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/simonz05/blobserver"
	"github.com/simonz05/blobserver/blob"
	"github.com/simonz05/blobserver/protocol"
	"github.com/simonz05/util/httputil"
	"github.com/simonz05/util/log"
)

const maxRemovesPerRequest = 1000

// createBatchRemoveHandler returns the handler that removes blobs
func createBatchRemoveHandler(storage blobserver.Storage) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		res, err := handleBatchRemove(r, storage)
		if err != nil {
			httputil.ServeJSONError(rw, err)
		} else {
			httputil.ReturnJSON(rw, res)
		}
	})
}

// createRemoveHandler returns the handler that removes blob a single blob at path
func createRemoveHandler(storage blobserver.Storage) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		res, err := handleRemove(r, storage)
		if err != nil {
			httputil.ServeJSONError(rw, err)
		} else {
			httputil.ReturnJSON(rw, res)
		}
	})
}

func handleBatchRemove(req *http.Request, storage blobserver.Storage) (interface{}, error) {
	res := new(protocol.RemoveResponse)
	n := 0
	toRemove := make([]blob.Ref, 0)

	for {
		n++
		if n > maxRemovesPerRequest {
			return nil, newRateLimitError(maxRemovesPerRequest)
		}

		key := fmt.Sprintf("blob%v", n)
		value := req.FormValue(key)
		ref, ok := blob.Parse(value)

		if !ok {
			break
		}

		toRemove = append(toRemove, ref)
	}

	err := storage.RemoveBlobs(toRemove)

	if err != nil {
		log.Errorf("Server error during remove: %v", err)
		return nil, newHTTPError("Server error", http.StatusInternalServerError)
	}

	res.Removed = toRemove
	return res, nil
}

func handleRemove(req *http.Request, storage blobserver.Storage) (interface{}, error) {
	res := new(protocol.RemoveResponse)
	vars := mux.Vars(req)
	ref, ok := blob.Parse(vars["blobRef"])
	if !ok {
		return nil, newHTTPError("Invalid blob ref", http.StatusBadRequest)
	}
	toRemove := []blob.Ref{ref}
	err := storage.RemoveBlobs(toRemove)

	if err != nil {
		log.Errorf("Server error during remove: %v", err)
		return nil, newHTTPError("Server error", http.StatusInternalServerError)
	}

	res.Removed = toRemove
	return res, nil
}
