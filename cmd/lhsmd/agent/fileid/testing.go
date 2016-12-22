// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package fileid

import "fmt"

type (
	fileMap map[string][]byte

	testManager struct {
		files fileMap
	}
)

func (m *testManager) update(p string, fileID []byte) error {
	return m.set(p, fileID)
}

func (m *testManager) set(p string, fileID []byte) error {
	m.files[p] = fileID

	return nil
}

func (m *testManager) get(p string) ([]byte, error) {
	if attr, ok := m.files[p]; ok {
		return attr, nil
	}
	return nil, fmt.Errorf("%s was not found in fileAttr map", p)
}

// EnableTestMode swaps out the real implementation for a test-friendly
// mock.
func EnableTestMode() {
	UUID = Attribute{&testManager{
		files: make(fileMap),
	}}
	Hash = Attribute{&testManager{
		files: make(fileMap),
	}}
	URL = Attribute{&testManager{
		files: make(fileMap),
	}}
}

// DisableTestMode re-enables normal operation.
func DisableTestMode() {
	defaultAttrs()
}
