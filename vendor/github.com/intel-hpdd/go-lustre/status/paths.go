// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package status

import (
	"path"

	"github.com/intel-hpdd/go-lustre"
	"github.com/intel-hpdd/go-lustre/fs"
	"github.com/intel-hpdd/go-lustre/llapi"
)

// FidPathname returns a path for a FID.
//
// Paths are relative from the fs.RootDir of the filesystem.
// If the fid is referred to by more than one file (i.e. hard links),
// the the LINKNO specifies a specific link to return. This does
// not update linkno on return. Use Paths to retrieve all hard link
// names.
//
func FidPathname(mnt fs.RootDir, f *lustre.Fid, linkno int) (string, error) {
	var recno int64
	return llapi.Fid2Path(mnt.Path(), f, &recno, &linkno)
}

// FidPathnames returns all paths for a fid.
//
// This returns a slice containing all names that reference
// the fid.
//
func FidPathnames(mnt fs.RootDir, f *lustre.Fid) ([]string, error) {
	return fidPathnames(mnt, f, false)
}

func fidPathnames(mnt fs.RootDir, f *lustre.Fid, absPath bool) ([]string, error) {
	var recno int64
	var linkno int
	var prevLinkno = -1
	var paths = make([]string, 0)
	for prevLinkno < linkno {
		prevLinkno = linkno
		p, err := llapi.Fid2Path(mnt.Path(), f, &recno, &linkno)
		if err != nil {
			return paths, err
		}

		if absPath {
			p = path.Join(mnt.Path(), p)
		}
		paths = append(paths, p)

	}
	return paths, nil
}

/*
// Pathnames returns all paths for a FID.
//
// This returns a slice containing all names that reference
// the FID.
//
func (f *fid) Pathnames(mnt fs.RootDir) ([]string, error) {
	return FidPathnames(mnt, f.String())
}

// AbsPathnames returns all paths for a FID.
//
// This returns a slice containing all names that reference
// the FID.
//
func (f *fid) AbsPathnames(mnt fs.RootDir) ([]string, error) {
	return FidAbsPathnames(mnt, f.String())
}

// Path returns the "open by fid" path.
func (f *fid) Path(mnt fs.RootDir) string {
	return FidPath(mnt, f.String())
}

// MarshalJSON converts a Fid to a string for JSON.
func (f *fid) MarshalJSON() ([]byte, error) {
	return []byte(`"` + f.String() + `"`), nil
}

// UnMarshalJSON converts fid string to Fid.
func (f *fid) UnMarshalJSON(b []byte) (err error) {
	newFid, err := ParseFid(string(b))
	f = newFid.(*fid)
	return err
}

// ParseFid converts a fid in string format to a Fid
func ParseFid(fidstr string) (Fid, error) {
	cfid, err := llapi.ParseFid(fidstr)
	if err != nil {
		return nil, err
	}
	return NewFid(cfid), nil
}

// FidAbsPathnames returns all paths for a FIDSTR.
//
// This returns a slice containing all names that reference
// the FID.
//
func FidAbsPathnames(mnt fs.RootDir, fidstr string) ([]string, error) {
	return fidPathnames(mnt, fidstr, true)
}

*/
