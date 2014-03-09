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
	"bytes"
	"fmt"
	"io"
	"mime"
	"net/http"

	"github.com/simonz05/blobserver"
	"github.com/simonz05/blobserver/blob"
	"github.com/simonz05/blobserver/protocol"
	"github.com/simonz05/util/log"
	"github.com/simonz05/util/httputil"
	"github.com/simonz05/util/readerutil"
)

// CreateUploadHandler returns the handler that receives multi-part form uploads
func newUploadHandler(storage blobserver.Storage) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleMultiPartUpload(w, r, storage)
	})
}

func handleMultiPartUpload(rw http.ResponseWriter, req *http.Request, blobReceiver blobserver.Storage) {
	res := new(protocol.UploadResponse)

	receivedBlobs := make([]blob.SizedRef, 0, 10)

	multipart, err := req.MultipartReader()
	if multipart == nil {
		httputil.BadRequestError(rw, fmt.Sprintf(
			"Expected multipart/form-data POST request; %v", err))
		return
	}

	var errBuf bytes.Buffer
	addError := func(s string) {
		log.Printf("Client error: %s", s)
		if errBuf.Len() > 0 {
			errBuf.WriteByte('\n')
		}
		errBuf.WriteString(s)
	}

	for {
		mimePart, err := multipart.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			addError(fmt.Sprintf("Error reading multipart section: %v", err))
			break
		}

		contentDisposition, params, err := mime.ParseMediaType(mimePart.Header.Get("Content-Disposition"))
		if err != nil {
			addError("invalid Content-Disposition")
			break
		}

		if contentDisposition != "form-data" {
			addError(fmt.Sprintf("Expected Content-Disposition of \"form-data\"; got %q", contentDisposition))
			break
		}

		formName := params["name"]
		ref, ok := blob.Parse(formName)
		if !ok {
			addError(fmt.Sprintf("Ignoring form key %q", formName))
			continue
		}

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
			addError(fmt.Sprintf("Error receiving blob %v: %v\n", ref, err))
			break
		}
		log.Printf("Received blob %v\n", blobGot)
		receivedBlobs = append(receivedBlobs, blobGot)
	}

	for _, got := range receivedBlobs {
		res.Received = append(res.Received, &protocol.RefAndSize{
			Ref:  got.Ref,
			Size: uint32(got.Size),
		})
	}

	res.ErrorText = errBuf.String()

	httputil.ReturnJSON(rw, res)
}
