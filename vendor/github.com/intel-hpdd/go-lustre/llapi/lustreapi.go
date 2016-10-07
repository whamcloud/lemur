// Copyright (c) 2016 Intel Corporation. All rights reserved.
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
	"syscall"
	"unsafe"
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
