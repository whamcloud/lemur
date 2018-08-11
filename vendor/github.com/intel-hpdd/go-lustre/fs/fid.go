// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package fs

import (
	"os"
	"path"

	"github.com/intel-hpdd/go-lustre"
	"github.com/intel-hpdd/go-lustre/luser"
)

// LookupFid returns the Fid for the given file or an error.
func LookupFid(path string) (*lustre.Fid, error) {
	return luser.GetFid(path)
}

/*
Slow version...
func LookupFid(path string) (*lustre.Fid, error) {
	fid, err := llapi.Path2Fid(path)
	if err != nil {
		return nil, fmt.Errorf("%s: fid not found (%s)", path, err.Error())
	}
	return fid, nil
}
*/

// FidPath returns the open-by-fid path for a fid.
func FidPath(mnt RootDir, f *lustre.Fid) string {
	return path.Join(mnt.Path(), FidRelativePath(f))
}

// FidRelativePath returns the relattive open-by-fid path for a fid.
func FidRelativePath(f *lustre.Fid) string {
	return path.Join(".lustre", "fid", f.String())
}

// StatFid returns an os.FileInfo given a mountpoint and fid
func StatFid(mnt RootDir, f *lustre.Fid) (os.FileInfo, error) {
	return os.Stat(FidPath(mnt, f))
}

// LstatFid returns an os.FileInfo given a mountpoint and fid
func LstatFid(mnt RootDir, f *lustre.Fid) (os.FileInfo, error) {
	return os.Lstat(FidPath(mnt, f))
}

// OpenByFid returns an open file handle given a mountpoint and fid
func OpenByFid(mnt RootDir, f *lustre.Fid) (*os.File, error) {
	return os.Open(FidPath(mnt, f))
}

// OpenFileByFid returns an open file handle given a mountpoint and fid
func OpenFileByFid(mnt RootDir, f *lustre.Fid, flags int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(FidPath(mnt, f), flags, perm)
}
