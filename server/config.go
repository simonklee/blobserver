// Copyright 2014 Simon Zimmermann. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package server

import (
	"net/http"

	"github.com/simonz05/blobserver"
	"github.com/simonz05/blobserver/protocol"
	"github.com/simonz05/util/httputil"
	"github.com/simonz05/util/log"
)

// createBatchRemoveHandler returns the handler that removes blobs
func createConfigHandler(storage blobserver.Storage) http.Handler {
	sc, ok := storage.(blobserver.StorageConfiger)
	if !ok {
		return http.HandlerFunc(notImplementedHandler)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleConfig(w, r, sc)
	})
}

func handleConfig(w http.ResponseWriter, req *http.Request, storage blobserver.StorageConfiger) {
	res := new(protocol.ConfigResponse)
	res.Data = storage.Config()
	log.Println("config:", res)
	httputil.ReturnJSON(w, res)
}
