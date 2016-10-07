// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package agent

import (
	"fmt"
	"os"
	"path"
	"time"

	"github.com/pkg/errors"

	"github.intel.com/hpdd/lemur/cmd/lhsmd/agent/fileid"
	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/lustre"
	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/hsm"
	"github.intel.com/hpdd/lustre/llapi"
	"github.intel.com/hpdd/lustre/status"
)

func createSnapDir(p string) (string, error) {
	fi, err := os.Lstat(p)
	if err != nil {
		return "", errors.Wrap(err, "lstat failed")
	}
	snapDir := path.Join(p, ".hsmsnap")
	err = os.MkdirAll(snapDir, fi.Mode())
	if err != nil {
		return "", errors.Wrap(err, "mkdir all failed")
	}
	return snapDir, nil
}

func createStubFile(f string, fi os.FileInfo, archive uint, layout *llapi.DataLayout) error {
	_, err := hsm.Import(f, archive, fi, layout)
	if err != nil {
		os.Remove(f)
		return errors.Wrapf(err, "%s: import failed", f)
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
			return errors.Wrap(err, "create snapdir failed")
		}
		fi, err := os.Lstat(absPath)
		if err != nil {
			return errors.Wrap(err, "lstat failed")
		}
		f := path.Join(snapDir, snapName(fi))
		if first {
			var layout *llapi.DataLayout
			layout, err = llapi.FileDataLayout(absPath)
			if err != nil {
				alert.Warnf("%s: unable to get layout: %v", f, err)
				return errors.Wrap(err, "get layout")
			}
			debug.Printf("%s: layout: %#v", absPath, layout)
			err = createStubFile(f, fi, archive, layout)
			if err != nil {
				return errors.Wrap(err, "create stub file")
			}
			err = fileid.Set(f, fileID)
			if err != nil {
				return errors.Wrapf(err, "%s: set fileid", f)
			}
			firstPath = f
			first = false
		} else {
			err = os.Link(firstPath, f)
			if err != nil {
				return errors.Wrapf(err, "%s: link to %s failed", f, firstPath)
			}
		}

	}
	return nil
}

func createSnapshot(mnt fs.RootDir, archive uint, fid *lustre.Fid, fileID []byte) error {
	names, err := status.FidPathnames(mnt, fid)
	if err != nil {
		return errors.Wrapf(err, "%s: fidpathname failed", fid)
	}

	return createSnapshots(mnt, archive, fileID, names)
}
