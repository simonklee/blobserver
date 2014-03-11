// Copyright 2014 Simon Zimmermann. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package server

type httpError struct {
	code    int
	message string
}

func (e httpError) HTTPCode() int {
	return e.code
}

func (e httpError) Error() string {
	return e.message
}

func newHttpError(message string, code int) httpError {
	return httpError{code: code, message: message}
}
