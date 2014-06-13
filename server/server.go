// Copyright 2014 Simon Zimmermann. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

// Package server implements HTTP interface for blobserver

package server

import (
	"net"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/simonz05/blobserver"
	"github.com/simonz05/util/handler"
	"github.com/simonz05/util/log"
	"github.com/simonz05/util/pat"
	"github.com/simonz05/util/sig"
)

func setupServer(storage blobserver.Storage) (err error) {
	router := mux.NewRouter()

	sub := router.PathPrefix("/v1/api/blobserver/blob").Subrouter()
	pat.Post(sub, "/upload/", createUploadHandler(storage))
	pat.Delete(sub, `/remove/{blobRef:[[:alnum:]_\/\.-]+}/`, createRemoveHandler(storage))
	pat.Post(sub, "/remove/", createBatchRemoveHandler(storage))
	pat.Get(sub, `/stat/{blobRef:[[:alnum:]_\/\.-]+}/`, createStatHandler(storage))
	pat.Head(sub, `/stat/{blobRef:[[:alnum:]_\/\.-]+}/`, createStatHandler(storage))
	pat.Get(sub, "/stat/", createBatchStatHandler(storage))

	sub = router.PathPrefix("/v1/api/blobserver").Subrouter()
	pat.Get(sub, "/config/", createConfigHandler(storage))

	router.StrictSlash(false)

	// global middleware
	wrapped := handler.Use(router, handler.LogHandler, handler.MeasureHandler, handler.RecoveryHandler)
	http.Handle("/", wrapped)
	return nil
}

func ListenAndServe(laddr string, storage blobserver.Storage) error {
	if err := setupServer(storage); err != nil {
		return err
	}

	l, err := net.Listen("tcp", laddr)

	if err != nil {
		return err
	}

	log.Printf("Listen on %s", l.Addr())

	sig.TrapCloser(l)
	err = http.Serve(l, nil)
	log.Printf("Shutting down ..")
	return err
}
