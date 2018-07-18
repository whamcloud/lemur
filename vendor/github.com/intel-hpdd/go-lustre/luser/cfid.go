// Copyright (c) 2016 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

// Package luser uses lustre interfaces exported to usersapce
// directly, instead of using the liblustreapi.a library.
// Data structures created mirror those defined in lustre_user.
package luser

import (
	"encoding/binary"

	"github.com/intel-hpdd/go-lustre"
	"github.com/intel-hpdd/go-lustre/pkg/xattr"
)

// XattrNameLMA is the name of extended attribute for the striping data.
const XattrNameLMA = "trusted.lma" // from lustre_idl.h

func getFid(getattr func(attr string, buf []byte) error) (*lustre.Fid, error) {
	buf := make([]byte, 64)
	err := getattr(XattrNameLMA, buf)
	if err != nil {
		return nil, err
	}
	// fid is buf + 8 offset
	fid := parseFid(buf[8:24], binary.LittleEndian)
	return &fid, nil
}

// GetFid retuns the lustre.Fid for the path name.
func GetFid(path string) (*lustre.Fid, error) {
	return getFid(func(attr string, buf []byte) error {
		_, err := xattr.Lgetxattr(path, attr, buf)
		return err
	})
}

// GetFidFd retuns the lustre.Fid for the path name.
func GetFidFd(fd int) (*lustre.Fid, error) {
	return getFid(func(attr string, buf []byte) error {
		_, err := xattr.Fgetxattr(fd, attr, buf)
		return err
	})
}
