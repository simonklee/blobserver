/*
Copyright 2011 Google Inc.

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

package blobserver

import (
	"fmt"
	"sync"

	"github.com/simonz05/blobserver/config"
)

// A StorageConstructor returns a Storage implementation from a configuration.
type StorageConstructor func(*config.Config) (Storage, error)

var mapLock sync.Mutex
var storageConstructors = make(map[string]StorageConstructor)

func RegisterStorageConstructor(typ string, ctor StorageConstructor) {
	mapLock.Lock()
	defer mapLock.Unlock()
	if _, ok := storageConstructors[typ]; ok {
		panic("blobserver: StorageConstructor already registered for type: " + typ)
	}
	storageConstructors[typ] = ctor
}

func CreateStorage(config *config.Config) (Storage, error) {
	mapLock.Lock()
	ctor, ok := storageConstructors[config.StorageType()]
	mapLock.Unlock()
	if !ok {
		return nil, fmt.Errorf("Storage type %s not known or loaded", config.StorageType())
	}
	return ctor(config)
}
