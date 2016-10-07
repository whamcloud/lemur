// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package layout

//
// #cgo LDFLAGS: -llustreapi
// #include <fcntl.h>
// #include <stdlib.h>
// #include <lustre/lustreapi.h>
import "C"
import "unsafe"

const (
	DEFAULT = uint64(C.LLAPI_LAYOUT_DEFAULT)
	RAID0   = uint64(C.LLAPI_LAYOUT_RAID0)
)

type Layout struct {
	layout *C.struct_llapi_layout
}

func New() *Layout {
	return &Layout{layout: C.llapi_layout_alloc()}
}

func (cl *Layout) Free() {
	C.llapi_layout_free(cl.layout)
}

type LayoutError error

func (l *Layout) StripeCount() uint64 {
	var i C.uint64_t
	rc := C.llapi_layout_stripe_count_get(l.layout, &i)
	if rc < 0 {
		return 0
	}
	return uint64(i)
}

func (l *Layout) StripeCountSet(c uint64) error {
	var i C.uint64_t
	i = C.uint64_t(c)
	rc, err := C.llapi_layout_stripe_count_set(l.layout, i)
	if rc < 0 {
		return LayoutError(err)
	}
	return nil
}

func (l *Layout) StripeSize() uint64 {
	var i C.uint64_t
	rc := C.llapi_layout_stripe_size_get(l.layout, &i)
	if rc < 0 {
		return 0
	}
	return uint64(i)
}

func (l *Layout) StripeSizeSet(c uint64) error {
	var i C.uint64_t
	i = C.uint64_t(c)
	rc, err := C.llapi_layout_stripe_size_set(l.layout, i)
	if rc < 0 {
		return LayoutError(err)
	}
	return nil
}

func (l *Layout) Pattern() uint64 {
	var i C.uint64_t
	rc := C.llapi_layout_pattern_get(l.layout, &i)
	if rc < 0 {
		return 0
	}
	return uint64(i)
}

func (l *Layout) PatternSet(c uint64) error {
	rc, err := C.llapi_layout_pattern_set(l.layout, C.uint64_t(c))
	if rc < 0 {
		return err
	}
	return nil
}

func (l *Layout) OstIndex(stripe uint64) (uint64, error) {
	var i C.uint64_t
	rc, err := C.llapi_layout_ost_index_get(l.layout, C.uint64_t(stripe), &i)
	if rc < 0 {
		return 0, err
	}
	return uint64(i), nil
}

func (l *Layout) OstIndexSet(stripe int, index uint64) error {
	rc, err := C.llapi_layout_ost_index_set(l.layout, C.int(stripe), C.uint64_t(index))
	if rc < 0 {
		return LayoutError(err)
	}
	return nil
}

func (l *Layout) FileOpen(path string, openFlags int, mode int) (int, error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	fd := C.llapi_layout_file_open(cpath, C.int(openFlags), C.mode_t(mode), l.layout)
	return int(fd), nil
}

func GetByPath(path string) (*Layout, error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	l, err := C.llapi_layout_get_by_path(cpath, 0)

	if err != nil {
		return nil, err
	}
	return &Layout{layout: l}, nil
}
