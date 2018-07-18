// Copyright (c) 2016 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

// Package llapi provides access to many of the functions avialable in liblustreapi.
//
//
package llapi

//
// #cgo LDFLAGS: -llustreapi
// #include <lustre/lustreapi.h>
// #include <stdlib.h>
//
import "C"
import (
	"math"
	"syscall"
	"unsafe"

	lustre "github.com/intel-hpdd/go-lustre"
	"github.com/pkg/errors"
)

func isError(rc C.int, err error) error {
	if rc < 0 {
		if err != nil {
			return err
		}
		return syscall.Errno(-rc)
	}
	return nil
}

func safeInt64(in uint64) (out int64, err error) {
	// The coordinator uses this value to signify EOF.
	if in == math.MaxUint64 {
		out = lustre.MaxExtentLength
		return
	}

	out = int64(in)
	if out < 0 {
		err = errors.Errorf("%d overflows int64", in)
	}

	return
}

// GetVersion returns the version of lustre installed on the host.
func GetVersion() (string, error) {
	var buffer [4096]C.char
	var cversion *C.char

	rc, err := C.llapi_get_version(&buffer[0], C.int(len(buffer)), &cversion)
	if err := isError(rc, err); err != nil {
		return "", err
	}

	return C.GoString(cversion), nil
}

// GetName returns the name-id of the client filesystem at mountPath
func GetName(mountPath string) (string, error) {
	var buffer [2048]C.char

	cmountPath := C.CString(mountPath)
	defer C.free(unsafe.Pointer(cmountPath))

	rc, err := C.llapi_getname(cmountPath, &buffer[0], C.size_t(len(buffer)))
	if err := isError(rc, err); err != nil {
		return "", err
	}

	return C.GoString(&buffer[0]), nil
}
