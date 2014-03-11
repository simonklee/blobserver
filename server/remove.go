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

func newBatchRemoveHandler(storage blobserver.Storage) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := handleBatchRemove(w, r, storage)
		if err != nil {
			httputil.ServeJSONError(w, err)
		}
	})
}

func handleBatchRemove(w http.ResponseWriter, req *http.Request, storage blobserver.Storage) error {
	if req.Method != "POST" {
		log.Fatalf("Invalid method; handlers misconfigured")
	}
	res := new(protocol.RemoveResponse)
	n := 0
	toRemove := make([]blob.Ref, 0)

	for {
		n++
		if n > maxRemovesPerRequest {
			return newHttpError(fmt.Sprintf("Too many removes in this request; max is %d", maxRemovesPerRequest), 400)
		}

		key := fmt.Sprintf("blob%v", n)
		value := req.FormValue(key)

		if value == "" {
			break
		}

		ref := blob.Ref{Id: value}
		toRemove = append(toRemove, ref)
	}

	err := storage.RemoveBlobs(toRemove)

	if err != nil {
		log.Errorf("Server error during remove: %v", err)
		return newHttpError("Server error", http.StatusInternalServerError)
	}

	res.Removed = toRemove
	httputil.ReturnJSON(w, res)
	return nil
}

func newRemoveHandler(storage blobserver.Storage) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := handleRemove(w, r, storage)
		if err != nil {
			httputil.ServeJSONError(w, err)
		}
	})
}

func handleRemove(w http.ResponseWriter, req *http.Request, storage blobserver.Storage) error {
	res := new(protocol.RemoveResponse)
	vars := mux.Vars(req)
	value := vars["ResourceID"]
	ref := blob.Ref{Id: value}
	toRemove := []blob.Ref{ref}
	err := storage.RemoveBlobs(toRemove)

	if err != nil {
		log.Errorf("Server error during remove: %v", err)
		return newHttpError("Server error", http.StatusInternalServerError)
	}

	httputil.ReturnJSON(w, res)
	return nil
}
