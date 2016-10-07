// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package lustre

import "fmt"

// Fid is a pure Go representation of a Lustre file identifier. It is
// intended to be the only representation of a Lustre Fid outside of
// llapi.
type Fid struct {
	Seq uint64
	Oid uint32
	Ver uint32
}

func (f *Fid) String() string {
	return fmt.Sprintf("[0x%x:0x%x:0x%x]", f.Seq, f.Oid, f.Ver)
}

// IsZero is true if Fid is 0.
func (f *Fid) IsZero() bool {
	return f.Seq == 0 && f.Oid == 0 && f.Ver == 0
}

// IsDotLustre is true if Fid is special .lustre entry.
func (f *Fid) IsDotLustre() bool {
	return f.Seq == 0x200000002 && f.Oid == 0x1 && f.Ver == 0x0
}

// MarshalJSON converts a Fid to a string for JSON.
func (f *Fid) MarshalJSON() ([]byte, error) {
	return []byte(`"` + f.String() + `"`), nil
}

// UnmarshalJSON converts fid string to Fid.
func (f *Fid) UnmarshalJSON(b []byte) (err error) {
	// trim the '"'
	if b[0] == '"' {
		b = b[1 : len(b)-1]
	}
	newFid, err := ParseFid(string(b))
	*f = *newFid
	return err
}

// ParseFid converts a fid in string format to a Fid
func ParseFid(fidstr string) (*Fid, error) {
	fid := &Fid{}
	if fidstr[0] == '[' {
		fidstr = fidstr[1 : len(fidstr)-1]
	}
	n, err := fmt.Sscanf(fidstr, "0x%x:0x%x:0x%x", &fid.Seq, &fid.Oid, &fid.Ver)
	if err != nil || n != 3 {
		return nil, fmt.Errorf("lustre: unable to parse fid string: %v", fidstr)
	}
	return fid, nil
}
