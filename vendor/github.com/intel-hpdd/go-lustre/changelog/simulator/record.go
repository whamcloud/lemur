// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package simulator

import (
	"bytes"
	"fmt"
	"time"

	"github.com/intel-hpdd/go-lustre"
)

type simRecord struct {
	index           int64
	name            string
	typeString      string
	typeCode        uint
	time            time.Time
	targetFid       *lustre.Fid
	parentFid       *lustre.Fid
	sourceName      string
	sourceFid       *lustre.Fid
	sourceParentFid *lustre.Fid
	isRename        bool
	isLastRename    bool
	isLastUnlink    bool
	hasCruft        bool
	jobID           string
}

func (r *simRecord) Index() int64 {
	return r.index
}

func (r *simRecord) Name() string {
	return r.name
}

func (r *simRecord) Type() string {
	return r.typeString
}

func (r *simRecord) TypeCode() uint {
	return r.typeCode
}

func (r *simRecord) Time() time.Time {
	return r.time
}

func (r *simRecord) TargetFid() *lustre.Fid {
	return r.targetFid
}

func (r *simRecord) ParentFid() *lustre.Fid {
	return r.parentFid
}

func (r *simRecord) SourceName() string {
	return r.sourceName
}

func (r *simRecord) SourceFid() *lustre.Fid {
	return r.sourceFid
}

func (r *simRecord) SourceParentFid() *lustre.Fid {
	return r.sourceParentFid
}

func (r *simRecord) IsRename() bool {
	return r.isRename
}

func (r *simRecord) IsLastRename() (bool, bool) {
	return r.isLastRename, r.hasCruft
}

func (r *simRecord) IsLastUnlink() (bool, bool) {
	return r.isLastUnlink, r.hasCruft
}

func (r *simRecord) JobID() string {
	return r.jobID
}

func (r *simRecord) String() string {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf("%d ", r.index))
	buf.WriteString(fmt.Sprintf("%02d%s ", r.typeCode, r.typeString))
	buf.WriteString(fmt.Sprintf("%s ", r.time))
	buf.WriteString(fmt.Sprintf("%#x ", 0))
	buf.WriteString("'' ")
	if len(r.jobID) > 0 {
		buf.WriteString(fmt.Sprintf("job=%s ", r.jobID))
	}
	if r.sourceFid != nil && !r.sourceFid.IsZero() {
		buf.WriteString(fmt.Sprintf("%s/%s", r.sourceParentFid,
			r.sourceFid))
		if r.sourceParentFid != r.parentFid {
			buf.WriteString(fmt.Sprintf("->%s/%s ",
				r.parentFid, r.targetFid))
		} else {
			buf.WriteString(" ")
		}
	} else {
		buf.WriteString(fmt.Sprintf("%s/%s ", r.parentFid, r.targetFid))
	}
	if len(r.sourceName) > 0 {
		buf.WriteString(fmt.Sprintf("%s->", r.sourceName))
	}
	if len(r.name) > 0 {
		buf.WriteString(r.name)
	}

	return buf.String()
}
