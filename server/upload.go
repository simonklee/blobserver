/*
Copyright 2013 The Camlistore Authors
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
	"encoding/hex"
	"fmt"
	"io"
	"mime"
	"net/http"

	"github.com/simonz05/blobserver"
	"github.com/simonz05/blobserver/blob"
	"github.com/simonz05/blobserver/protocol"
	"github.com/simonz05/util/httputil"
	"github.com/simonz05/util/log"
	"github.com/simonz05/util/readerutil"
)

// createUploadHandler returns the handler that receives multi-part form uploads.
func createUploadHandler(storage blobserver.Storage) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		res, err := handleMultiPartUpload(r, storage)
		if err != nil {
			log.Errorf("upload: %v", err)
			httputil.ServeJSONError(rw, err)
		} else {
			httputil.ReturnJSONCode(rw, http.StatusCreated, res)
		}
	})
}

func handleMultiPartUpload(req *http.Request, blobReceiver blobserver.Storage) (interface{}, error) {
	res := new(protocol.UploadResponse)
	receivedBlobs := make([]blob.SizedRef, 0, 4)
	multipart, err := req.MultipartReader()

	if err != nil {
		return nil, newHTTPError(fmt.Sprintf("Expected multipart/form-data POST request; %v", err), http.StatusBadRequest)
	}

	useFilename := false
	req.ParseForm()

	if req.FormValue("use-filename") != "" {
		useFilename = true
	}

	for {
		mimePart, err := multipart.NextPart()

		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, newHTTPError(fmt.Sprintf("Error reading multipart section: %v", err), http.StatusBadRequest)
		}

		contentDisposition, _, err := mime.ParseMediaType(mimePart.Header.Get("Content-Disposition"))

		if err != nil {
			return nil, newHTTPError("Invalid Content-Disposition", http.StatusBadRequest)
		}

		if contentDisposition != "form-data" {
			return nil, newHTTPError(fmt.Sprintf("Expected Content-Disposition of \"form-data\"; got %q", contentDisposition), http.StatusBadRequest)
		}

		var ref blob.Ref
		var tooBig int64 = blobserver.MaxBlobSize + 1
		var readBytes int64

		filename := mimePart.FileName()
		log.Println("filename:", filename)

		if useFilename {
			log.Println("Use filename")
			ref = blob.NewRefFilename(filename)
		} else {
			ref = blob.NewRef(filename)
		}

		blobGot, err := blobReceiver.ReceiveBlob(ref, &readerutil.CountingReader{
			Reader: io.LimitReader(mimePart, tooBig),
			N:      &readBytes,
		})

		if readBytes == tooBig {
			err = fmt.Errorf("blob over the limit of %d bytes", blobserver.MaxBlobSize)
		}

		if err != nil {
			var errmsg string

			if log.Severity >= log.LevelInfo {
				errmsg = fmt.Sprintf("Error receiving blob (read bytes: %d) %v: %v\n", readBytes, ref, err)
			} else {
				errmsg = fmt.Sprintf("Error receiving blob: %v\n", err)
			}

			return nil, newHTTPError(errmsg, http.StatusInternalServerError)
		}

		log.Printf("Received blob %v\n", blobGot)
		receivedBlobs = append(receivedBlobs, blobGot)
	}

	for _, got := range receivedBlobs {
		rv := protocol.RefInfo{
			Ref:  got.Ref,
			Size: uint32(got.Size),
		}
		if h := got.Hash(); h != nil {
			rv.MD5 = hex.EncodeToString(h.Sum(nil))
		}
		res.Received = append(res.Received, rv)
	}
	return res, nil
}
