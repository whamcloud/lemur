// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package llapi

/*
#include <sys/ioctl.h>  // Needed for LL_IOC_HSM_IMPORT definition
#include <lustre/lustreapi.h>
#include <errno.h>
#include <stdlib.h>

__u16 _lum_layout_gen(struct lov_user_md_v1 *lum) {
        return lum->lmm_layout_gen;
}

__u16 _lum_set_offset(struct lov_user_md_v1 *lum, __u16 offset) {
        return lum->lmm_stripe_offset = offset;
}

struct lov_user_ost_data *_lum_object_at_v1(struct lov_user_md_v1 *lum, int index) {
        if (index > lum->lmm_stripe_count) {
                return NULL;
        }
        return &lum->lmm_objects[index];
}

struct lov_user_ost_data *_lum_object_at_v3(struct lov_user_md_v1 *lum, int index) {
        struct lov_user_md_v3 *lumv3 =  (struct lov_user_md_v3 *)lum;
        if (index > lumv3->lmm_stripe_count) {
                return NULL;
        }
        return &lumv3->lmm_objects[index];
}

char  *_lum_pool_name(struct lov_user_md_v1 *lum) {
        struct lov_user_md_v3 *lumv3 =  (struct lov_user_md_v3 *)lum;
        return &lumv3->lmm_pool_name[0];
}

void _lum_set_pool_name(struct lov_user_md_v1 *lum, char * pool_name) {
        struct lov_user_md_v3 *lumv3 =  (struct lov_user_md_v3 *)lum;
	strncpy(lumv3->lmm_pool_name, pool_name, LOV_MAXPOOLNAME);
}


struct lu_fid *_lov_user_ost_fid(struct lov_user_ost_data *luod) {
        return &luod->l_ost_oi.oi_fid;
}

*/
import "C"

import (
	"os"
	"unsafe"

	"github.com/intel-hpdd/go-lustre"
	"github.com/intel-hpdd/go-lustre/pkg/xattr"
)

type (
	// OstData is an element of a stripe layout
	OstData struct {
		Object lustre.Fid
		Gen    int
		Index  int
	}

	// DataLayout is the structure for file data.
	DataLayout struct {
		StripePattern int
		StripeSize    int
		StripeCount   int
		StripeOffset  int
		Generation    int
		PoolName      string
		Objects       []OstData
	}
)

func allocLum() *C.struct_lov_user_md_v1 {
	maxLumSize := C.lov_user_md_size(C.LOV_MAX_STRIPE_COUNT, C.LOV_USER_MAGIC_V3)
	buf := make([]byte, maxLumSize)
	return (*C.struct_lov_user_md_v1)(unsafe.Pointer(&buf[0]))
}

func getObjectAtV1(lum *C.struct_lov_user_md_v1, index int) *C.struct_lov_user_ost_data {
	return C._lum_object_at_v1(lum, C.int(index))
}

func getObjectAtV3(lum *C.struct_lov_user_md_v1, index int) *C.struct_lov_user_ost_data {
	return C._lum_object_at_v3(lum, C.int(index))
}

func layoutFromLum(lum *C.struct_lov_user_md_v1) (*DataLayout, error) {
	l := &DataLayout{
		StripePattern: int(lum.lmm_pattern),
		StripeSize:    int(lum.lmm_stripe_size),
		StripeCount:   int(lum.lmm_stripe_count),
		Generation:    int(C._lum_layout_gen(lum)),
		StripeOffset:  -1,
	}

	getObjectAt := getObjectAtV1

	if lum.lmm_magic == C.LOV_USER_MAGIC_V3 {
		getObjectAt = getObjectAtV3
		l.PoolName = C.GoString(C._lum_pool_name(lum))
	}

	if (l.StripePattern & C.LOV_PATTERN_F_RELEASED) == 0 {
		for i := 0; i < l.StripeCount; i++ {
			var o OstData
			cobj := getObjectAt(lum, i)
			if cobj == nil {
				break
			}
			o.Gen = int(cobj.l_ost_gen)
			o.Index = int(cobj.l_ost_idx)
			cfid := C._lov_user_ost_fid(cobj)
			o.Object = *fromCFid(cfid)
			l.Objects = append(l.Objects, o)
		}
	}
	return l, nil
}

// FileDataLayout retrieves the file's data layout
func FileDataLayout(name string) (*DataLayout, error) {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	lum := allocLum()

	rc, err := C.llapi_file_get_stripe(cName, (*C.struct_lov_user_md_v1)(unsafe.Pointer(lum)))
	if err := isError(rc, err); err != nil {
		return nil, err
	}

	return layoutFromLum(lum)
}

// FileDataLayoutEA retrieves the file's data layout from the extended attribute.
func FileDataLayoutEA(name string) (*DataLayout, error) {
	maxLumSize := C.lov_user_md_size(C.LOV_MAX_STRIPE_COUNT, C.LOV_USER_MAGIC_V3)
	b1 := make([]byte, maxLumSize)

	sz, err := xattr.Lgetxattr(name, "lustre.lov", b1)
	if err != nil {
		return nil, err
	}
	lovBuf := b1[0:sz]
	lum := (*C.struct_lov_user_md)(unsafe.Pointer(&lovBuf[0]))

	return layoutFromLum(lum)
}

// SetFileLayout sets the data striping layout on a file that has been
// created with O_LOV_DELAY_CREATE
func SetFileLayout(fd int, layout *DataLayout) error {
	maxLumSize := C.lov_user_md_size(C.LOV_MAX_STRIPE_COUNT, C.LOV_USER_MAGIC_V3)
	buf := make([]byte, maxLumSize)
	lum := (*C.struct_lov_user_md)(unsafe.Pointer(&buf[0]))

	lumSize := lumFromLayout(layout, lum)

	return xattr.Fsetxattr(fd, "lustre.lov", buf[:lumSize], xattr.CREATE)
}

// This is only done when writing a layout, so we don't need to copy
// all the fields.
func lumFromLayout(layout *DataLayout, lum *C.struct_lov_user_md_v1) int {
	lum.lmm_magic = C.LOV_USER_MAGIC_V1
	// Clear released flag when writing the layout
	lum.lmm_pattern = C.__u32(layout.StripePattern ^ C.LOV_PATTERN_F_RELEASED)
	lum.lmm_stripe_size = C.__u32(layout.StripeSize)
	lum.lmm_stripe_count = C.__u16(layout.StripeCount)
	C._lum_set_offset(lum, C.__u16(layout.StripeOffset))
	if layout.PoolName != "" {
		cPoolName := C.CString(layout.PoolName)
		defer C.free(unsafe.Pointer(cPoolName))
		lum.lmm_magic = C.LOV_USER_MAGIC_V3
		C._lum_set_pool_name(lum, cPoolName)
	}

	size := C.lov_user_md_size(0, lum.lmm_magic)
	return int(size)
}

// FileOpenPool creates a new file with provided layout
func FileOpenPool(name string, flags int, mode uint32, layout *DataLayout) (int, error) {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	var cPoolName *C.char
	if layout.PoolName != "" {
		cPoolName = C.CString(layout.PoolName)
		defer C.free(unsafe.Pointer(cPoolName))
	}

	fd, err := C.llapi_file_open_pool(cName, C.int(flags), C.int(mode), C.ulonglong(layout.StripeSize), C.int(layout.StripeOffset), C.int(layout.StripeCount), C.int(layout.StripePattern), cPoolName)
	if err := isError(fd, err); err != nil {
		return 0, err
	}
	return int(fd), nil
}

// DirDataLayout returns the default DataLayout on a directory.
func DirDataLayout(name string) (*DataLayout, error) {
	lum := allocLum()
	dir, err := os.Open(name)
	if err != nil {
		return nil, err
	}

	_, err = ioctl(int(dir.Fd()), C.LL_IOC_LOV_GETSTRIPE, uintptr(unsafe.Pointer(lum)))
	if err != nil {
		return nil, err
	}
	return layoutFromLum(lum)
}
