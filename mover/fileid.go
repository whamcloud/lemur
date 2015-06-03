package main

import (
	"errors"

	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/system"

	"code.google.com/p/go-uuid/uuid"
)

var errNoFileId = errors.New("No file id")

func fileUrl(mnt fs.RootDir, p string) (string, error) {
	urlBytes, err := system.Lgetxattr(p, "user.hsm_url")
	if err != nil {
		return "", err
	}
	return string(urlBytes), nil
}

func newFileId(mnt fs.RootDir, p string) (string, error) {
	uuid := uuid.New()
	err := system.Lsetxattr(p, "user.hsm_guid", []byte(uuid), 0)
	if err != nil {
		return "", err
	}
	return uuid, nil
}

func getFileId(mnt fs.RootDir, path string) (string, error) {
	uuid, err := fileID(mnt, path)
	if err != nil {
		uuid, err = newFileId(mnt, path)
		if err != nil {
			return "", err
		}
	}
	return uuid, nil
}

func fileID(mnt fs.RootDir, path string) (string, error) {
	uuid, err := system.Lgetxattr(path, "user.hsm_guid")
	if err != nil {

		return "", err
	}
	return string(uuid), nil
}

func setFileId(p string, id string) error {
	err := system.Lsetxattr(p, "user.hsm_guid", []byte(id), 0)
	if err != nil {
		return err
	}
	return nil
}
