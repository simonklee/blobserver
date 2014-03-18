/*
Copyright 2014 The Camlistore Authors
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

// Package protocol contains types for Camlistore protocol types.
package protocol

import (
	"encoding/json"

	"github.com/simonz05/blobserver"
	"github.com/simonz05/blobserver/blob"
)

type RefInfo struct {
	blob.Ref
	Size uint32
	MD5  string `json:"MD5,omitempty"`
}

// UploadResponse is the JSON document returned from the blob batch
// upload handler.
type UploadResponse struct {
	Received []RefInfo         `json:"Data"`
	Error    map[string]string `json:"Error,omitempty"`
}

func (p *UploadResponse) MarshalJSON() ([]byte, error) {
	v := *p
	if v.Received == nil {
		v.Received = []RefInfo{}
	}
	return json.Marshal(v)
}

// RemoveResponse is the JSON document returned from the blob batch
// remove handler.
type RemoveResponse struct {
	Removed []blob.Ref        `json:"Data"`
	Error   map[string]string `json:"Error,omitempty"`
}

func (p *RemoveResponse) MarshalJSON() ([]byte, error) {
	v := *p
	if v.Removed == nil {
		v.Removed = []blob.Ref{}
	}
	return json.Marshal(v)
}

// ConfigResponse is the JSON document returned storage config handler.
type ConfigResponse struct {
	Data  *blobserver.Config `json:"Data"`
	Error map[string]string  `json:"Error,omitempty"`
}
