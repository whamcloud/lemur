// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package llapi

/*
#include <lustre/lustreapi.h>
#include <stdlib.h>

// This doesn't exist in the API, but maybe it should?
struct hsm_user_state *_hsm_user_state_alloc()
{
	int len = 0;

	len += sizeof(struct hsm_user_state);
	len += sizeof(struct hsm_extent);

	return (struct hsm_user_state *)malloc(len);
}
*/
import "C"

import (
	"fmt"
	"strings"
	"unsafe"
)

type (
	// HsmFileState is a bitmask containing the hsm state(s) for a file.
	HsmFileState uint32

	// HsmStateFlag represents a given HSM state flag
	HsmStateFlag uint32
)

// HSM State flags
const (
	HsmFileExists    = HsmStateFlag(C.HS_EXISTS)
	HsmFileArchived  = HsmStateFlag(C.HS_ARCHIVED)
	HsmFileReleased  = HsmStateFlag(C.HS_RELEASED)
	HsmFileDirty     = HsmStateFlag(C.HS_DIRTY)
	HsmFileNoRelease = HsmStateFlag(C.HS_NORELEASE)
	HsmFileNoArchive = HsmStateFlag(C.HS_NOARCHIVE)
	HsmFileLost      = HsmStateFlag(C.HS_LOST)
)

// HsmStateFlags is a map of HsmStateFlag -> string
// NB: There's no llapi.hsm_state2name(), so we have to do it ourselves...
var HsmStateFlags = map[HsmStateFlag]string{
	HsmFileExists:    "exists",
	HsmFileArchived:  "archived",
	HsmFileReleased:  "released",
	HsmFileDirty:     "dirty",
	HsmFileNoRelease: "no_release",
	HsmFileNoArchive: "no_archive",
	HsmFileLost:      "lost",
}

func (s HsmFileState) String() string {
	return fmt.Sprintf("%#x %s", s, strings.Join(s.Flags(), " "))
}

func (f HsmStateFlag) String() string {
	return HsmStateFlags[f]
}

// HasFlag checks to see if the supplied flag matches
func (s HsmFileState) HasFlag(flag HsmStateFlag) bool {
	return uint32(s)&uint32(flag) > 0
}

// Flags returns a list of flag strings
func (s HsmFileState) Flags() []string {
	var flagStrings []string

	for flag, str := range HsmStateFlags {
		if s.HasFlag(flag) {
			flagStrings = append(flagStrings, str)
		}
	}

	return flagStrings
}

// GetHsmFileStatus returns the HSM state and archive number for the given file.
func GetHsmFileStatus(filePath string) (HsmFileState, uint32, error) {
	hus := C._hsm_user_state_alloc()
	defer C.free(unsafe.Pointer(hus))

	buf := C.CString(filePath)
	defer C.free(unsafe.Pointer(buf))
	rc, err := C.llapi_hsm_state_get(buf, hus)
	if err != nil {
		return 0, 0, err
	}
	if rc > 0 {
		return 0, 0, fmt.Errorf("Got %d from llapi_hsm_state_get, expected 0", rc)
	}
	return HsmFileState(hus.hus_states), uint32(hus.hus_archive_id), nil
}

// SetHsmFileStatus updates the file's HSM flags and/or archive ID
func SetHsmFileStatus(filePath string, setMask, clearMask uint64, archiveID uint32) error {
	buf := C.CString(filePath)
	defer C.free(unsafe.Pointer(buf))

	rc, err := C.llapi_hsm_state_set(buf, C.__u64(setMask), C.__u64(clearMask), C.__u32(archiveID))
	if rc > 0 {
		return fmt.Errorf("Got %d from llapi_hsm_state_set, expected 0", rc)
	}
	return err
}
