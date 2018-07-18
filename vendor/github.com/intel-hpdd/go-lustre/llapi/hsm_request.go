// Copyright (c) 2016 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package llapi

//
// #include <fcntl.h>
// #include <stdlib.h>
// #include <lustre/lustreapi.h>
//
// /* CGO 1.5 doesn't support zero byte fields at the
//  * end of structs so we need an accessor.
//  */
// struct hsm_user_item *_hur_user_item(struct hsm_user_request  *hur) {
//     return &hur->hur_user_item[0];
// }
import "C"
import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/intel-hpdd/go-lustre"
)

// MaxBatchSize is a limit imposed by liblustreapi, somewhere.
var MaxBatchSize = 50

// HsmUserAction specifies an action for HsmRequest().
type HsmUserAction uint

//HSM Action Types
// Each of these actions are applied to up to MaxBatchSize files in a single request. Each file
// in the request is processed indepentently and potentially in any order.
const (
	// HsmUserNone: Noop
	HsmUserNone = HsmUserAction(C.HUA_NONE)
	// HsmUserArchive: Archive file to specified archive id
	HsmUserArchive = HsmUserAction(C.HUA_ARCHIVE)
	// HsmUserRelease: Remove file's data from the filesystem. (Must have been achived first.)
	HsmUserRelease = HsmUserAction(C.HUA_RELEASE)
	// HsmUserRestore: Restore a released file
	HsmUserRestore = HsmUserAction(C.HUA_RESTORE)
	// HsmUserRemove: Remove data from the archive. (File must not be released.)
	HsmUserRemove = HsmUserAction(C.HUA_REMOVE)
	// HsmUserCancel: Cancels current in progress rqeuest for the file.
	HsmUserCancel = HsmUserAction(C.HUA_CANCEL)
)

func (action HsmUserAction) String() string {
	return C.GoString(C.hsm_user_action2name(C.enum_hsm_user_action(action)))
}

// HsmRequest submits an HSM request for list of files
func HsmRequest(r string, cmd HsmUserAction, archiveID uint, fidsToSend []*lustre.Fid) (int, error) {
	if len(fidsToSend) < 1 {
		return 0, fmt.Errorf("lustre: Request must include at least 1 file")
	}

	var sentCount int

	for len(fidsToSend) > 0 {
		var batch []*lustre.Fid
		if len(fidsToSend) < MaxBatchSize {
			batch = fidsToSend
		} else {
			batch = fidsToSend[:MaxBatchSize]
		}

		batchCount, err := hsmRequest(r, cmd, archiveID, batch)
		sentCount += batchCount
		if err != nil {
			return sentCount, err
		}

		fidsToSend = fidsToSend[batchCount:]
	}

	return sentCount, nil
}

func hsmRequest(r string, cmd HsmUserAction, archiveID uint, fids []*lustre.Fid) (int, error) {
	fileCount := len(fids)
	hur := C.llapi_hsm_user_request_alloc(C.int(fileCount), 0)
	defer C.free(unsafe.Pointer(hur))
	if hur == nil {
		panic("Failed to allocate HSM User Request struct!")
	}

	hur.hur_request.hr_action = C.__u32(cmd)
	hur.hur_request.hr_archive_id = C.__u32(archiveID)
	hur.hur_request.hr_flags = 0
	hur.hur_request.hr_itemcount = 0
	hur.hur_request.hr_data_len = 0

	// https://code.google.com/p/go-wiki/wiki/cgo#Turning_C_arrays_into_Go_slices
	hdr := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(C._hur_user_item(hur))),
		Len:  fileCount,
		Cap:  fileCount,
	}
	userItems := *(*[]C.struct_hsm_user_item)(unsafe.Pointer(&hdr))
	for i, f := range fids {
		userItems[i].hui_extent.offset = 0
		userItems[i].hui_extent.length = C.__u64(^uint(0))
		userItems[i].hui_fid = *toCFid(f)
		hur.hur_request.hr_itemcount++
	}

	num := int(hur.hur_request.hr_itemcount)
	if num != fileCount {
		return 0, fmt.Errorf("lustre: Can't submit incomplete request (%d/%d)", num, fileCount)
	}

	buf := C.CString(r)
	defer C.free(unsafe.Pointer(buf))
	rc, err := C.llapi_hsm_request(buf, hur)
	if err := isError(rc, err); err != nil {
		return 0, fmt.Errorf("lustre: Got error from llapi_hsm_request: %s", err.Error())
	}

	return num, nil
}
