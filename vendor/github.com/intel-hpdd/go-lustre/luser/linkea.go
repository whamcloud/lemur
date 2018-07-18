// Copyright (c) 2016 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package luser

import (
	"encoding/binary"
	"fmt"

	"github.com/intel-hpdd/go-lustre"
	"github.com/intel-hpdd/go-lustre/pkg/xattr"
)

const (
	linkEAMagic uint32 = 0x11EAF1DF
	maxEASize          = 4096
)

// LinkEntry is an entry from the link extended attribute
// on a Lustre file. Each entry represents a name for
// hardlink and parent directory that conatains that name.
type LinkEntry struct {
	Name   string
	Parent lustre.Fid
}

func parseFid(buf []byte, swab binary.ByteOrder) (fid lustre.Fid) {
	fid.Seq = swab.Uint64(buf[0:8])
	fid.Oid = swab.Uint32(buf[8:12])
	fid.Ver = swab.Uint32(buf[12:16])
	return
}

// GetLinkEA returns the link extended attribute for a file or error if
// there is no link attribute.
// Pretty fragile since we're hardcoding all the struct sizes here.
func GetLinkEA(path string) ([]LinkEntry, error) {
	var buf [maxEASize]byte
	_, err := xattr.Lgetxattr(path, "trusted.link", buf[:])
	if err != nil {
		return nil, err
	}
	var swab binary.ByteOrder
	// read struct link_ea_header
	if binary.BigEndian.Uint32(buf[0:4]) == linkEAMagic {
		swab = binary.BigEndian
	} else if binary.LittleEndian.Uint32(buf[0:4]) == linkEAMagic {
		swab = binary.LittleEndian
	} else {
		return nil, fmt.Errorf("%s: Invalid EA", path)
	}
	reccount := swab.Uint32(buf[4:8])
	max := swab.Uint64(buf[8:16])

	entries := make([]LinkEntry, reccount)

	// sizeof(struct link_ea_header) = 24
	i := 24

	// now read the list of struct link_ea_entry
	for n := 0; n < len(entries) && uint64(i) < max; n++ {
		// code comment says these are always in BigEndian
		reclen := binary.BigEndian.Uint16(buf[i : i+2])
		entries[n].Parent = parseFid(buf[i+2:i+18], binary.BigEndian)

		entries[n].Name = string(buf[i+18 : i+int(reclen)])
		i += int(reclen)
	}
	return entries, nil
}
