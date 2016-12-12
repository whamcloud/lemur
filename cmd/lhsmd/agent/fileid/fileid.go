// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package fileid

import (
	"github.com/intel-hpdd/go-lustre"
	"github.com/intel-hpdd/go-lustre/fs"
	"github.com/intel-hpdd/go-lustre/pkg/xattr"
	"github.com/intel-hpdd/logging/debug"
)

const xattrUUID = "trusted.lhsm_uuid"

var mgr manager = newManager(xattrUUID)

type (
	manager interface {
		update(fs.RootDir, *lustre.Fid, []byte) error
		set(string, []byte) error
		get(fs.RootDir, *lustre.Fid) ([]byte, error)
	}
	attrManager struct {
		attr string
	}
)

// Manager returns a new attrManager
func newManager(attr string) *attrManager {
	return &attrManager{attr: attr}
}

func (m *attrManager) update(mnt fs.RootDir, fid *lustre.Fid, fileID []byte) error {
	p := fs.FidPath(mnt, fid)

	return m.set(p, fileID)
}

func (m *attrManager) set(p string, fileID []byte) error {
	return xattr.Lsetxattr(p, m.attr, fileID, 0)
}

func (m *attrManager) get(mnt fs.RootDir, fid *lustre.Fid) ([]byte, error) {
	buf := make([]byte, 256)
	p := fs.FidPath(mnt, fid)

	sz, err := xattr.Lgetxattr(p, m.attr, buf)
	if err != nil {
		return nil, err
	}
	return buf[0:sz], nil
}

// Update updates an existing fileid attribute with a new value
func Update(mnt fs.RootDir, fid *lustre.Fid, fileID []byte) error {
	return mgr.update(mnt, fid, fileID)
}

// Set sets a fileid attribute on a file
func Set(p string, fileID []byte) error {
	debug.Printf("setting %s=%s on %s", xattrUUID, fileID, p)
	return mgr.set(p, fileID)
}

// Get gets the fileid attribute for a file
func Get(mnt fs.RootDir, fid *lustre.Fid) ([]byte, error) {
	return mgr.get(mnt, fid)
}
