package agent

import (
	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/lustre"
	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/pkg/xattr"
)

const xattrFileID = "trusted.hsm_file_id"

func updateFileID(mnt fs.RootDir, fid *lustre.Fid, fileID []byte) error {
	p := fs.FidPath(mnt, fid)

	return setFileID(p, fileID)
}

func setFileID(p string, fileID []byte) error {
	debug.Printf("setting %s=%s on %s", xattrFileID, fileID, p)
	return xattr.Lsetxattr(p, xattrFileID, fileID, 0)
}

func getFileID(mnt fs.RootDir, fid *lustre.Fid) ([]byte, error) {
	buf := make([]byte, 256)
	p := fs.FidPath(mnt, fid)

	sz, err := xattr.Lgetxattr(p, xattrFileID, buf)
	if err != nil {
		return nil, err
	}
	return buf[0:sz], nil
}
