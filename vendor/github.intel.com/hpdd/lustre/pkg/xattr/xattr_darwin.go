package xattr

import (
	"syscall"
	"unsafe"
)

var _zero uintptr

// Lgetxattr returns the extended attribute from the path name.
func Lgetxattr(path, attr string, dest []byte) (sz int, err error) {
	pathBuf, err := syscall.BytePtrFromString(path)
	if err != nil {
		return
	}

	attrBuf, err := syscall.BytePtrFromString(attr)
	if err != nil {
		return
	}

	var buf unsafe.Pointer
	if len(dest) > 0 {
		buf = unsafe.Pointer(&dest[0])
	} else {
		buf = unsafe.Pointer(&_zero)
	}

	rc, _, errno := syscall.Syscall6(syscall.SYS_GETXATTR,
		uintptr(unsafe.Pointer(pathBuf)),
		uintptr(unsafe.Pointer(attrBuf)),
		uintptr(buf),
		uintptr(len(dest)),
		0,
		0)

	sz = int(rc)
	if errno != 0 {
		err = errno
	}
	return
}

// Fgetxattr returns the extended attribute from the path name.
func Fgetxattr(fd int, attr string, dest []byte) (sz int, err error) {
	attrBuf, err := syscall.BytePtrFromString(attr)
	if err != nil {
		return
	}

	var buf unsafe.Pointer
	if len(dest) > 0 {
		buf = unsafe.Pointer(&dest[0])
	} else {
		buf = unsafe.Pointer(&_zero)
	}

	rc, _, errno := syscall.Syscall6(syscall.SYS_FGETXATTR,
		uintptr(fd),
		uintptr(unsafe.Pointer(attrBuf)),
		uintptr(buf),
		uintptr(len(dest)),
		0,
		0)

	sz = int(rc)
	if errno != 0 {
		err = errno
	}
	return
}

// Lsetxattr sets the extended attribute on the path name
func Lsetxattr(path, attr string, value []byte, flags int) (err error) {
	pathBuf, err := syscall.BytePtrFromString(path)
	if err != nil {
		return
	}

	attrBuf, err := syscall.BytePtrFromString(attr)
	if err != nil {
		return
	}

	var valuePtr unsafe.Pointer
	if len(value) > 0 {
		valuePtr = unsafe.Pointer(&value[0])
	} else {
		valuePtr = unsafe.Pointer(&_zero)
	}
	_, _, errno := syscall.Syscall6(syscall.SYS_SETXATTR,
		uintptr(unsafe.Pointer(pathBuf)),
		uintptr(unsafe.Pointer(attrBuf)),
		uintptr(valuePtr),
		uintptr(len(value)),
		uintptr(flags),
		0)
	if errno != 0 {
		err = errno
	}
	return
}

// Lsetxattr sets the extended attribute on the path name
func Fsetxattr(fd int, attr string, value []byte, flags int) error {
	attrBuf, err := syscall.BytePtrFromString(attr)
	if err != nil {
		return err
	}

	valuePtr := &value[0]

	_, _, errno := syscall.Syscall6(syscall.SYS_FSETXATTR,
		uintptr(fd),
		uintptr(unsafe.Pointer(attrBuf)),
		uintptr(unsafe.Pointer(valuePtr)),
		uintptr(len(value)),
		uintptr(flags),
		0)
	if errno == 0 {
		return nil
	}
	return errno
}
