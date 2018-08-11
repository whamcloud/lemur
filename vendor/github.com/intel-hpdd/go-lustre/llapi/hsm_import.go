// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package llapi

/*
// #include <sys/types.h>
// #include <sys/stat.h>
// #include <unisted.h>
#include <fcntl.h>      // Needed for C.O_LOV_DELAY_CREATE definition
#include <sys/ioctl.h>  // Needed for LL_IOC_HSM_IMPORT definition
#include <stdlib.h>
#include <lustre/lustreapi.h>

void _lum_set_stripe_offset(struct lov_user_md_v3 *lum, __u16 offset) {
	lum->lmm_stripe_offset = offset;
}
*/
import "C"

import (
	"errors"
	"os"
	"syscall"
	"unsafe"

	"github.com/intel-hpdd/go-lustre"
	"github.com/intel-hpdd/go-lustre/luser"
	"golang.org/x/sys/unix"
)

var errStatError = errors.New("stat failure")

// The nsec fields in stat_t are defined differently between EL6 and
// EL7, and Go's C compiler complains about this. Worked around this
// by reimplmenting HsmImport in Go, below. It seems silly to create a
// stat struct only for it to be turned into the hsm_user_import,
// anyway. Leaving original code here as a reference and in case this
// doesn't work out.

/*
func statToCstat(fi os.FileInfo) *C.struct_stat {
	stat, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		debug.Print("no stat info")
		return nil
	}

	var st C.struct_stat
	st.st_uid = C.__uid_t(stat.Uid)
	st.st_gid = C.__gid_t(stat.Gid)
	st.st_mode = C.__mode_t(stat.Mode)
	st.st_size = C.__off_t(stat.Size)
	st.st_mtim.tv_sec = C.__time_t(stat.Mtim.Sec)
	st.st_mtim.tv_nsec = C.__syscall_slong_t(stat.Mtim.Nsec)
	st.st_atim.tv_sec = C.__time_t(stat.Atim.Sec)
	st.st_atim.tv_nsec = C.__syscall_slong_t(stat.Atim.Nsec)

	return &st
}
*/

// HsmImport creates a placeholder file in Lustre that refers to the
// file contents stored in an HSM backend.  The file is created in the
// "released" state, and the contents will be retrieved when the file is opened
// or an explicit restore is requested.
//
// TODO: using an os.FileInfo to pass the file metadata doesn't work for all cases. This
// should be simple struct the caller can populate. (Though just using syscall.Stat_t
// is also tempting.)
func HsmImport(name string, archive uint, fi os.FileInfo, layout *DataLayout) (*lustre.Fid, error) {

	return hsmImport(name, archive, fi, layout)

	// Orignal llapi version
	/*
		var cfid C.lustre_fid

		st := statToCstat(fi)
		if st == nil {
			return nil, errStatError
		}

		cname := C.CString(name)
		defer C.free(unsafe.Pointer(cname))

		rc, err := C.llapi_hsm_import(
			cname,
			C.int(archive),
			st,
			C.ulonglong(layout.StripeSize),
			C.int(layout.StripeOffset),
			C.int(layout.StripeCount),
			C.int(layout.StripePattern),
			nil,
			&cfid,
		)
		if rc < 0 {
			return nil, err
		}
		return fromCFid(&cfid), nil
	*/
}

// Go version of llapi_hsm_import.
func hsmImport(name string, archive uint, fi os.FileInfo, layout *DataLayout) (*lustre.Fid, error) {
	// Create file with no layout
	stat, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, errStatError
	}

	if layout == nil {
		layout = DefaultDataLayout()
	}

	layout.StripePattern = layout.StripePattern | C.LOV_PATTERN_F_RELEASED
	fd, err := FileOpenPool(name, unix.O_CREAT|unix.O_WRONLY, stat.Mode, layout)
	if err != nil {
		return nil, err
	}
	defer unix.Close(fd)

	fid, err := luser.GetFidFd(fd)
	if err != nil {
		return nil, err
	}

	// HSM_IMPORT
	var hui C.struct_hsm_user_import
	hui.hui_uid = C.__u32(stat.Uid)
	hui.hui_gid = C.__u32(stat.Gid)
	hui.hui_mode = C.__u32(stat.Mode)
	hui.hui_size = C.__u64(stat.Size)
	hui.hui_archive_id = C.__u32(archive)
	hui.hui_atime = C.__u64(stat.Atim.Sec)
	hui.hui_atime_ns = C.__u32(stat.Atim.Nsec)
	hui.hui_mtime = C.__u64(stat.Mtim.Sec)
	hui.hui_mtime_ns = C.__u32(stat.Mtim.Nsec)
	_, err = ioctl(fd, C.LL_IOC_HSM_IMPORT, uintptr(unsafe.Pointer(&hui)))
	if err != nil {
		return nil, err
	}

	return fid, nil
}
