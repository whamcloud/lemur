// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package llapi

import (
	"os"
	"syscall"
	"unsafe"
)

func ioctl(fd int, request, argp uintptr) (int, error) {
	rc, _, errorp := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), request, argp)
	if rc < 0 || errorp != 0 {
		return 0, os.NewSyscallError("ioctl", errorp)
	}
	return int(rc), nil
}

func sizeof(v interface{}) int {
	return int(unsafe.Sizeof(v))
}
