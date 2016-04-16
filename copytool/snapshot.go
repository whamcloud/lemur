package main

import (
	"fmt"
	"os"
	"path"
	"time"

	"github.intel.com/hpdd/lustre/fs"
)

func createSnapDir(p string) (string, error) {
	fi, err := os.Lstat(p)
	if err != nil {
		return "", err
	}
	snapDir := path.Join(p, ".hsmsnap")
	err = os.MkdirAll(snapDir, fi.Mode())
	if err != nil {
		return "", err
	}
	return snapDir, nil
}

func createStubFile(f string, fi os.FileInfo, archive uint) error {
	/*	_, err := hsm.Import(f, archive, fi, 0, 0, 1, 0, "")
		if err != nil {
			glog.Error("Import failed", err)
			return err
		}
	*/
	return nil
}

func createSnapshots(mnt fs.RootDir, archive uint, file_key string, names []string) error {
	var firstPath string
	first := true
	for _, p := range names {
		absPath := mnt.Join(p)
		snapDir, err := createSnapDir(path.Dir(absPath))
		if err != nil {
			return err
		}
		fi, err := os.Lstat(absPath)
		if err != nil {
			return err
		}
		f := path.Join(snapDir, fmt.Sprintf("%s^%s", fi.Name(), fi.ModTime().Format(time.RFC3339)))
		if first {
			err = createStubFile(f, fi, archive)
			setFileId(f, file_key)
			firstPath = f
			first = false
		} else {
			err = os.Link(firstPath, f)
		}

		if err != nil {
			return err
		}
	}
	return nil
}
