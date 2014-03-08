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
		BadRequestError(rw, fmt.Sprintf(
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

	ReturnJSON(rw, res)
}
