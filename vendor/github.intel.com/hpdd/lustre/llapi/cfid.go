package llapi

// This file contains code for interfacing with liblustreapi's Fid functions
// as well as functions for converting between native C Fids and pure Go Fids.

// #include <sys/ioctl.h>
// #include <stdlib.h>
// #include <lustre/lustreapi.h>
//
import "C"
import (
	"fmt"
	"os"
	"unsafe"

	"github.intel.com/hpdd/lustre"
)

func fromCFid(f *C.struct_lu_fid) *lustre.Fid {
	return &lustre.Fid{
		Seq: uint64(f.f_seq),
		Oid: uint32(f.f_oid),
		Ver: uint32(f.f_ver),
	}
}

func toCFid(fid *lustre.Fid) *C.struct_lu_fid {
	return &C.struct_lu_fid{
		f_seq: C.__u64(fid.Seq),
		f_oid: C.__u32(fid.Oid),
		f_ver: C.__u32(fid.Ver),
	}
}

// Path2Fid returns the Fid for the given path or an error.
func Path2Fid(path string) (*lustre.Fid, error) {
	cfid := &C.lustre_fid{}

	pathStr := C.CString(path)
	defer C.free(unsafe.Pointer(pathStr))

	rc, err := C.llapi_path2fid(pathStr, cfid)
	if err := isError(rc, err); err != nil {
		return nil, fmt.Errorf("%s: fid not found (%s)", path, err.Error())
	}
	return fromCFid(cfid), nil
}

// FidPathError is an error that occurs while retrieving the pathname for a fid.
//
type FidPathError struct {
	Fid *lustre.Fid
	Rc  int
	Err error
}

func (e *FidPathError) Error() string {
	return fmt.Sprintf("fid2path: %s failed: %d %v", e.Fid, e.Rc, e.Err)
}

// Fid2Path returns next path for given fid.
// This returns relative paths from the root of the filesystem.
func Fid2Path(device string, f *lustre.Fid, recno *int64, linkno *int) (string, error) {
	var buffer [4096]C.char
	var clinkno = C.int(*linkno)

	devStr := C.CString(device)
	defer C.free(unsafe.Pointer(devStr))
	fidStr := C.CString(f.String())
	defer C.free(unsafe.Pointer(fidStr))

	rc, err := C.llapi_fid2path(devStr, fidStr, &buffer[0],
		C.int(len(buffer)), (*C.longlong)(recno), &clinkno)
	*linkno = int(clinkno)
	if err := isError(rc, err); err != nil {
		return "", &FidPathError{f, int(rc), err}
	}
	p := C.GoString(&buffer[0])

	// This is a relative path, so make sure it doesn't start with a '/'
	if p[0] == '/' {
		p = p[1:]
	}
	return p, err
}

// GetMdtIndexByFid returns the MDT index for a given Fid
func GetMdtIndexByFid(mountFd int, f *lustre.Fid) (int, error) {
	var mdtIndex C.int

	rc, err := C.llapi_get_mdt_index_by_fid(C.int(mountFd), toCFid(f), &mdtIndex)
	if err := isError(rc, err); err != nil {
		return 0, err
	}

	return int(mdtIndex), nil
}

// GetMdtIndex returns the MDT the file resides on.
func GetMdtIndex(f *os.File, fid *lustre.Fid) (int, error) {
	return ioctl(int(f.Fd()), C.LL_IOC_FID2MDTIDX, uintptr(unsafe.Pointer(fid)))
}
