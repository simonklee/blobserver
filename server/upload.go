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
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := handleMultiPartUpload(w, r, storage)
		if err != nil {
			httputil.ServeJSONError(w, err)
		}
	})
}

func handleMultiPartUpload(rw http.ResponseWriter, req *http.Request, blobReceiver blobserver.Storage) error {
	res := new(protocol.UploadResponse)
	receivedBlobs := make([]blob.SizedRef, 0, 4)
	multipart, err := req.MultipartReader()

	if err != nil {
		return newHttpError(fmt.Sprintf("Expected multipart/form-data POST request; %v", err), 400)
	}

	for {
		mimePart, err := multipart.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return newHttpError(fmt.Sprintf("Error reading multipart section: %v", err), 400)
		}

		contentDisposition, _, err := mime.ParseMediaType(mimePart.Header.Get("Content-Disposition"))
		if err != nil {
			return newHttpError("Invalid Content-Disposition", 400)
		}
		if contentDisposition != "form-data" {
			return newHttpError(fmt.Sprintf("Expected Content-Disposition of \"form-data\"; got %q", contentDisposition), 400)
		}

		ref := blob.NewRef(mimePart.FileName())
		var tooBig int64 = blobserver.MaxBlobSize + 1
		var readBytes int64
		blobGot, err := blobReceiver.ReceiveBlob(ref, &readerutil.CountingReader{
			io.LimitReader(mimePart, tooBig),
			&readBytes,
		})
		if readBytes == tooBig {
			err = fmt.Errorf("blob over the limit of %d bytes", blobserver.MaxBlobSize)
		}
		if err != nil {
			return newHttpError(fmt.Sprintf("Error receiving blob %v: %v\n", ref, err), 400)
		}
		log.Printf("Received blob %v\n", blobGot)
		receivedBlobs = append(receivedBlobs, blobGot)
	}

	res.Received = receivedBlobs
	httputil.ReturnJSONCode(rw, 201, res)
	return nil
}
