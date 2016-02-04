package agent

import (
	"github.intel.com/hpdd/lustre"
	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/system"
)

func updateFileID(mnt fs.RootDir, fid *lustre.Fid, fileId []byte) error {
	p := fs.FidPath(mnt, fid)

	err := system.Lsetxattr(p, "user.hsm_file_id", fileId, 0)
	if err != nil {
		return err
	}
	return nil

}

func getFileID(mnt fs.RootDir, fid *lustre.Fid) ([]byte, error) {
	p := fs.FidPath(mnt, fid)

	id, err := system.Lgetxattr(p, "user.hsm_file_id")
	if err != nil {
		return nil, err
	}
	return id, nil
}
