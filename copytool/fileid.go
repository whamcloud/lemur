package main

import (
	"errors"

	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/lustre"
	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/pkg/xattr"

	"code.google.com/p/go-uuid/uuid"
)

var errNoFileId = errors.New("No file id")

func fileUrl(mnt fs.RootDir, fid *lustre.Fid) (string, error) {
	p := fs.FidPath(mnt, fid)
	urlBytes := make([]byte, 256)
	_, err := xattr.Lgetxattr(p, "user.hsm_url", urlBytes)
	if err != nil {
		return "", err
	}
	return string(urlBytes), nil
}

func newFileId(mnt fs.RootDir, fid *lustre.Fid) (string, error) {
	p := fs.FidPath(mnt, fid)
	uuid := uuid.New()
	err := xattr.Lsetxattr(p, "user.hsm_guid", []byte(uuid), 0)
	if err != nil {
		debug.Printf("xattr failed: %v", err)
		return "", err
	}
	return uuid, nil
}

func getFileId(mnt fs.RootDir, fid *lustre.Fid) (string, error) {
	uuid, err := fileID(mnt, fid)
	if err != nil {
		uuid, err = newFileId(mnt, fid)
		if err != nil {
			return "", err
		}
	}
	return uuid, nil
}

func fileID(mnt fs.RootDir, fid *lustre.Fid) (string, error) {
	p := fs.FidPath(mnt, fid)
	uuid := make([]byte, 32)
	_, err := xattr.Lgetxattr(p, "user.hsm_guid", uuid)
	if err != nil {

		return "", err
	}
	return string(uuid), nil
}

func setFileId(p string, id string) error {
	err := xattr.Lsetxattr(p, "user.hsm_guid", []byte(id), 0)
	if err != nil {
		return err
	}
	return nil
}
