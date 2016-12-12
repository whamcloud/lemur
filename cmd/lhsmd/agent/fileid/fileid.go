// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package fileid

import (
	"fmt"

	"github.com/intel-hpdd/go-lustre"
	"github.com/intel-hpdd/go-lustre/fs"
	"github.com/intel-hpdd/go-lustre/pkg/xattr"
	"github.com/intel-hpdd/logging/debug"
	"github.com/pkg/errors"
)

const xattrUUID = "trusted.lhsm_uuid"
const xattrHash = "trusted.lhsm_hash"
const xattrURL = "trusted.lhsm_url"

type (
	manager interface {
		update(fs.RootDir, *lustre.Fid, []byte) error
		set(string, []byte) error
		get(fs.RootDir, *lustre.Fid) ([]byte, error)
	}
	attrManager struct {
		attr string
	}
	Attribute struct {
		mgr manager
	}
)

var UUID, Hash, URL Attribute

func init() {
	defaultAttrs()
}
func defaultAttrs() {
	UUID = Attribute{newManager(xattrUUID)}
	Hash = Attribute{newManager(xattrHash)}
	URL = Attribute{newManager(xattrURL)}
}

// Manager returns a new attrManager
func newManager(attr string) *attrManager {
	return &attrManager{attr: attr}
}

func (m *attrManager) String() string {
	return m.attr
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

func (a Attribute) String() string {
	return fmt.Sprintf("%s", a.mgr)
}

// Update updates an existing fileid attribute with a new value
func (a Attribute) Update(mnt fs.RootDir, fid *lustre.Fid, fileID []byte) error {
	return a.mgr.update(mnt, fid, fileID)
}

// Set sets a fileid attribute on a file
func (a Attribute) Set(p string, fileID []byte) error {
	debug.Printf("setting %s=%s on %s", xattrUUID, fileID, p)
	return a.mgr.set(p, fileID)
}

// Get gets the fileid attribute for a file
func (a Attribute) Get(mnt fs.RootDir, fid *lustre.Fid) ([]byte, error) {
	val, err := a.mgr.get(mnt, fid)
	if err != nil {
		debug.Printf("Error reading attribute: %v (%s) will retry", err, a.mgr)
		// WTF, let's try again
		//time.Sleep(1 * time.Second)
		val, err = a.mgr.get(mnt, fid)
		if err != nil {
			return nil, errors.Wrap(err, a.String())
		}
	}
	return val, nil
}
