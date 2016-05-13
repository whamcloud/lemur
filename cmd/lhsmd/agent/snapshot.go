package agent

import (
	"fmt"
	"os"
	"path"
	"time"

	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/lustre"
	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/hsm"
	"github.intel.com/hpdd/lustre/llapi"
	"github.intel.com/hpdd/lustre/status"
	"github.intel.com/hpdd/policy/pdm/lhsmd/agent/fileid"
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

func createStubFile(f string, fi os.FileInfo, archive uint, layout *llapi.DataLayout) error {
	_, err := hsm.Import(f, archive, fi, layout)
	if err != nil {
		alert.Warnf("Import failed: %v", err)
		os.Remove(f)
		return err
	}
	return nil
}

func snapName(fi os.FileInfo) string {
	return fmt.Sprintf("%s^%s", fi.Name(), fi.ModTime().Format(time.RFC3339))
}

func createSnapshots(mnt fs.RootDir, archive uint, fileID []byte, names []string) error {
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
		f := path.Join(snapDir, snapName(fi))
		if first {
			var layout *DataLayout
			layout, err = llapi.FileDataLayout(absPath)
			if err != nil {
				alert.Warnf("%s: unable to get layout: %v", f, err)
				return err
			}
			debug.Printf("%s: layout: %#v", absPath, layout)
			err = createStubFile(f, fi, archive, layout)
			if err != nil {
				return err
			}
			err = fileid.Set(f, fileID)
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

func createSnapshot(mnt fs.RootDir, archive uint, fid *lustre.Fid, fileID []byte) error {
	names, err := status.FidPathnames(mnt, fid)
	if err != nil {
		return err
	}

	return createSnapshots(mnt, archive, fileID, names)
}
