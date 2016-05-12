package fileid

import (
	"fmt"

	"github.intel.com/hpdd/lustre"
	"github.intel.com/hpdd/lustre/fs"
)

type (
	fileMap map[string][]byte

	testManager struct {
		files fileMap
	}
)

func (m *testManager) update(mnt fs.RootDir, fid *lustre.Fid, fileID []byte) error {
	p := fs.FidRelativePath(fid)
	return m.set(p, fileID)
}

func (m *testManager) set(p string, fileID []byte) error {
	m.files[p] = fileID

	return nil
}

func (m *testManager) get(mnt fs.RootDir, fid *lustre.Fid) ([]byte, error) {
	p := fs.FidRelativePath(fid)

	if attr, ok := m.files[p]; ok {
		return attr, nil
	}
	return nil, fmt.Errorf("%s was not found in fileAttr map", p)
}

// EnableTestMode swaps out the real implementation for a test-friendly
// mock.
func EnableTestMode() {
	mgr = &testManager{
		files: make(fileMap),
	}
}

// DisableTestMode re-enables normal operation.
func DisableTestMode() {
	mgr = &testManager{}
}
