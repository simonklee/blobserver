package server

import (
	"net/http"

	"github.com/simonz05/blobserver/storage"
)

// CreateUploadHandler returns the handler that receives multi-part form uploads
func newUploadHandler(storage storage.Storage) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleMultiPartUpload(w, r, storage)
	})
}


func handleMultiPartUpload(rw http.ResponseWriter, req *http.Request, backend storage.Storage) {
}

