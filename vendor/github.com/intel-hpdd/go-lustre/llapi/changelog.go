// Copyright (c) 2016 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package llapi

// #include <stdlib.h>
// #include <lustre/lustreapi.h>
//
// /* cr_tfid is a union, so cgo essentially ignores it */
// lustre_fid _changelog_rec_tfid(struct changelog_rec *rec) {
//    return rec->cr_tfid;
// }
//
import "C"

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"time"
	"unsafe"

	"github.com/intel-hpdd/go-lustre"
)

// HsmEvent is a convenience type to represent an HSM event reported
// in a changelog record's flags.
type HsmEvent int32

func (he *HsmEvent) String() string {
	switch *he {
	case C.HE_ARCHIVE:
		return "Archive"
	case C.HE_RESTORE:
		return "Restore"
	case C.HE_CANCEL:
		return "Cancel"
	case C.HE_RELEASE:
		return "Release"
	case C.HE_REMOVE:
		return "Remove"
	case C.HE_STATE:
		return "Changed State"
	case C.HE_SPARE1:
		return "Spare1"
	case C.HE_SPARE2:
		return "Spare2"
	default:
		return fmt.Sprintf("Unknown event: %d", *he)
	}
}

// Changelog is opaque data representing an open changelog.
type Changelog struct {
	priv *byte
}

// ChangelogStart opens the changelog. The firsst record read will be
// the idnex specified by startRec.
func ChangelogStart(device string, startRec int64, follow bool) (*Changelog, error) {
	cl := Changelog{}
	// NB: CHANGELOG_FLAG_JOBID will be mandatory in future releases.
	// CHANGELOG_FLAG_BLOCK seems to be ignored? Can we remove it?
	flags := C.CHANGELOG_FLAG_BLOCK | C.CHANGELOG_FLAG_JOBID

	// NB: CHANGELOG_FLAG_FOLLOW is broken and hasn't worked for a
	// long time. This code is here in case it ever starts working
	// again.
	if follow {
		flags |= C.CHANGELOG_FLAG_FOLLOW
	}

	cDevice := C.CString(device)
	defer C.free(unsafe.Pointer(cDevice))

	rc := C.llapi_changelog_start((*unsafe.Pointer)(unsafe.Pointer(&cl.priv)),
		uint32(flags), cDevice, C.longlong(startRec))
	if rc != 0 {
		return nil, fmt.Errorf("Got nonzero RC from llapi_changelog_start: %d", rc)
	}

	return &cl, nil
}

// ChangelogFini closes the Changelog.
func ChangelogFini(cl *Changelog) error {
	rc := C.llapi_changelog_fini((*unsafe.Pointer)(unsafe.Pointer(&cl.priv)))
	if rc != 0 {
		return fmt.Errorf("Got nonzero RC from llapi_changelog_fini: %d", rc)
	}

	cl.priv = nil
	return nil
}

// ChangelogRecv returns the next record in the changelog.
func ChangelogRecv(cl *Changelog) (*ChangelogRecord, error) {
	var rec *C.struct_changelog_rec

	// 0 is valid message, < 0 is error code, 1 is EOF
	rc := C.llapi_changelog_recv(unsafe.Pointer(cl.priv), &rec)
	if rc == 1 {
		return nil, io.EOF
	} else if rc != 0 {
		return nil, fmt.Errorf("Got nonzero RC from llapi_changelog_recv: %d", rc)
	}

	r, err := newRecord(rec)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// ChangelogClear deletes all changelog records up to endRec.
func ChangelogClear(device string, token string, endRec int64) error {
	cDevice := C.CString(device)
	defer C.free(unsafe.Pointer(cDevice))
	cToken := C.CString(token)
	defer C.free(unsafe.Pointer(cToken))

	rc := C.llapi_changelog_clear(cDevice, cToken, C.longlong(endRec))
	if rc != 0 {
		return fmt.Errorf("Got nonzero RC from llapi_changelog_clear: %d", rc)
	}
	return nil
}

// Changelog Types
const (
	OpMark     = C.CL_MARK
	OpCreate   = C.CL_CREATE   /* namespace */
	OpMkdir    = C.CL_MKDIR    /* namespace */
	OpHardlink = C.CL_HARDLINK /* namespace */
	OpSoftlink = C.CL_SOFTLINK /* namespace */
	OpMknod    = C.CL_MKNOD    /* namespace */
	OpUnlink   = C.CL_UNLINK   /* namespace */
	OpRmdir    = C.CL_RMDIR    /* namespace */
	OpRename   = C.CL_RENAME   /* namespace */
	OpExt      = C.CL_EXT      /* namespace extended record (2nd half of rename) */
	OpOpen     = C.CL_OPEN     /* not currently used */
	OpClose    = C.CL_CLOSE    /* may be written to log only with mtime change */
	OpLayout   = C.CL_LAYOUT   /* file layout/striping modified */
	OpTrunc    = C.CL_TRUNC
	OpSetattr  = C.CL_SETATTR
	OpXattr    = C.CL_XATTR
	OpHSM      = C.CL_HSM   /* HSM specific events, see flags */
	OpMtime    = C.CL_MTIME /* Precedence: setattr > mtime > ctime > atime */
	OpCtime    = C.CL_CTIME
	OpAtime    = C.CL_ATIME
	OpLast     = C.CL_LAST
)

// ChangelogRecord is a record in a Changelog
type ChangelogRecord struct {
	name            string
	flags           uint
	index           int64
	prev            uint
	time            time.Time
	rType           uint
	typeName        string
	targetFid       *lustre.Fid
	parentFid       *lustre.Fid
	sourceName      string
	sourceFid       *lustre.Fid
	sourceParentFid *lustre.Fid
	jobID           string
}

// Index returns the changelog record's index in the log
func (r *ChangelogRecord) Index() int64 {
	return r.index
}

// Name returns the filename associated with the record (if available)
func (r *ChangelogRecord) Name() string {
	return r.name
}

// Type returns the changelog record's type as a string
func (r *ChangelogRecord) Type() string {
	return r.typeName
}

// TypeCode returns the changelog record's type as a string
func (r *ChangelogRecord) TypeCode() uint {
	return r.rType
}

// Time returns the changelog record's time
func (r *ChangelogRecord) Time() time.Time {
	return r.time
}

// TargetFid returns the recipient Fid for the changelog record's action
func (r *ChangelogRecord) TargetFid() *lustre.Fid {
	return r.targetFid
}

// ParentFid returns the parent Fid for the changelog record's action
func (r *ChangelogRecord) ParentFid() *lustre.Fid {
	return r.parentFid
}

// SourceFid returns the source Fid when a file is renamed
func (r *ChangelogRecord) SourceFid() *lustre.Fid {
	return r.sourceFid
}

// SourceParentFid returns the source Fid's parent Fid when a file is renamed
func (r *ChangelogRecord) SourceParentFid() *lustre.Fid {
	return r.sourceParentFid
}

// SourceName returns the source filename when a file is renamed
func (r *ChangelogRecord) SourceName() string {
	return r.sourceName
}

// IsRename is true if this record is a rename.
func (r *ChangelogRecord) IsRename() bool {
	return r.flags&C.CLF_RENAME == C.CLF_RENAME
}

// JobID returns the changelog record's Job ID information (if available)
func (r *ChangelogRecord) JobID() string {
	return r.jobID
}

func (r *ChangelogRecord) String() string {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf("%d ", r.index))
	buf.WriteString(fmt.Sprintf("%02d%s ", r.rType, r.typeName))
	buf.WriteString(fmt.Sprintf("%s ", r.time))
	buf.WriteString(fmt.Sprintf("%#x ", r.flags&C.CLF_FLAGMASK))
	buf.WriteString(fmt.Sprintf("%s ", strings.Join(r.flagStrings(), ",")))
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

func (r *ChangelogRecord) flagStrings() []string {
	var flagStrings []string

	switch r.rType {
	case OpHSM:
		event := HsmEvent(C.hsm_get_cl_event(C.__u16(r.flags)))
		flagStrings = append(flagStrings, event.String())
		hsmFlags := C.hsm_get_cl_flags(C.int(r.flags))
		switch hsmFlags {
		case C.CLF_HSM_DIRTY:
			flagStrings = append(flagStrings, "Dirty")
		}
	case OpUnlink:
		last, exists := r.IsLastUnlink()
		if last {
			flagStrings = append(flagStrings, "Last Hardlink Removed")
		}
		if exists {
			flagStrings = append(flagStrings, "Exists in Archive")
		}
	case OpRename:
		last, exists := r.IsLastRename()
		if last {
			flagStrings = append(flagStrings, "Last Hardlink Renamed")
		}
		if exists {
			flagStrings = append(flagStrings, "Exists in Archive")
		}
	}

	return flagStrings
}

// IsLastUnlink returns a tuple of boolean values to indicate:
// 1) Whether or not the unlink was for the the last hardlink
// 2) Whether or not there may still be an archive of the file in HSM
func (r *ChangelogRecord) IsLastUnlink() (last, exists bool) {
	if r.rType == OpUnlink {
		last = r.flags&C.CLF_UNLINK_LAST > 0
		exists = r.flags&C.CLF_UNLINK_HSM_EXISTS > 0
	}
	return
}

// IsLastRename returns a tuple of boolean values to indicate:
// 1) Whether or not the rename was for the the last hardlink
// 2) Whether or not there may still be an archive of the file in HSM
func (r *ChangelogRecord) IsLastRename() (last, exists bool) {
	if r.rType == OpRename {
		last = r.flags&C.CLF_RENAME_LAST > 0
		exists = r.flags&C.CLF_RENAME_LAST_EXISTS > 0
	}
	return
}

func hasJobID(r *ChangelogRecord) bool {
	return r.flags&C.CLF_JOBID == C.CLF_JOBID
}

func newRecord(cRec *C.struct_changelog_rec) (*ChangelogRecord, error) {
	tfid := C._changelog_rec_tfid(cRec)
	record := &ChangelogRecord{
		name:      C.GoString(C.changelog_rec_name(cRec)),
		index:     int64(cRec.cr_index),
		rType:     uint(cRec.cr_type),
		typeName:  C.GoString(C.changelog_type2str(C.int(cRec.cr_type))),
		flags:     uint(cRec.cr_flags),
		prev:      uint(cRec.cr_prev),
		time:      time.Unix(int64(cRec.cr_time>>30), 0), // WTF?
		targetFid: fromCFid(&tfid),
		parentFid: fromCFid(&cRec.cr_pfid),
	}
	if record.IsRename() {
		rename := C.changelog_rec_rename(cRec)
		record.sourceName = C.GoString(C.changelog_rec_sname(cRec))
		record.sourceFid = fromCFid(&rename.cr_sfid)
		record.sourceParentFid = fromCFid(&rename.cr_spfid)
	}
	if hasJobID(record) {
		jobid := C.changelog_rec_jobid(cRec)
		record.jobID = C.GoString(&jobid.cr_jobid[0])
	}

	return record, nil
}
