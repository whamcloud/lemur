package agent

import (
	"github.intel.com/hpdd/lustre"
	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/pkg/xattr"
)

func updateFileID(mnt fs.RootDir, fid *lustre.Fid, fileID []byte) error {
	p := fs.FidPath(mnt, fid)

	err := xattr.Lsetxattr(p, "trusted.hsm_file_id", fileID, 0)
	if err != nil {
		return err
	}
	return nil

}

func getFileID(mnt fs.RootDir, fid *lustre.Fid) ([]byte, error) {
	p := fs.FidPath(mnt, fid)

	id, err := xattr.Lgetxattr(p, "trusted.hsm_file_id")
	if err != nil {
		return nil, err
	}
	return id, nil
}
