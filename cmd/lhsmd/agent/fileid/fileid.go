package fileid

import (
	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/lustre"
	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/pkg/xattr"
)

const xattrFileID = "trusted.hsm_file_id"

var mgr manager = &fileIDManager{}

type (
	manager interface {
		update(fs.RootDir, *lustre.Fid, []byte) error
		set(string, []byte) error
		get(fs.RootDir, *lustre.Fid) ([]byte, error)
	}
	fileIDManager struct{}
)

func (m *fileIDManager) update(mnt fs.RootDir, fid *lustre.Fid, fileID []byte) error {
	p := fs.FidPath(mnt, fid)

	return m.set(p, fileID)
}

func (m *fileIDManager) set(p string, fileID []byte) error {
	return xattr.Lsetxattr(p, xattrFileID, fileID, 0)
}

func (m *fileIDManager) get(mnt fs.RootDir, fid *lustre.Fid) ([]byte, error) {
	buf := make([]byte, 256)
	p := fs.FidPath(mnt, fid)

	sz, err := xattr.Lgetxattr(p, xattrFileID, buf)
	if err != nil {
		return nil, err
	}
	return buf[0:sz], nil
}

// Update updates an existing fileid attribute with a new value
func Update(mnt fs.RootDir, fid *lustre.Fid, fileID []byte) error {
	return mgr.update(mnt, fid, fileID)
}

// Set sets a fileid attribute on a file
func Set(p string, fileID []byte) error {
	debug.Printf("setting %s=%s on %s", xattrFileID, fileID, p)
	return mgr.set(p, fileID)
}

// Get gets the fileid attribute for a file
func Get(mnt fs.RootDir, fid *lustre.Fid) ([]byte, error) {
	return mgr.get(mnt, fid)
}
