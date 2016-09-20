package status

//
// #cgo LDFLAGS: -llustreapi
// #include <lustre/lustreapi.h>
// #include <stdlib.h>
//
import "C"

import "unsafe"

// LovName returns the uniqe name for the LOV devcie for the client associated with the path.
func LovName(p string) (string, error) {
	var obd C.struct_obd_uuid
	cpath := C.CString(p)
	defer C.free(unsafe.Pointer(cpath))
	rc, err := C.llapi_file_get_lov_uuid(cpath, &obd)
	if rc < 0 || err != nil {
		return "", err
	}
	s := C.GoString(&obd.uuid[0])
	return s, nil
}

// LmvName returns the uniqe name for the LMV device for the client associated with the path.
func LmvName(p string) (string, error) {
	var obd C.struct_obd_uuid
	cpath := C.CString(p)
	defer C.free(unsafe.Pointer(cpath))
	rc, err := C.llapi_file_get_lmv_uuid(cpath, &obd)
	if rc < 0 || err != nil {
		return "", err
	}
	s := C.GoString(&obd.uuid[0])
	return s, nil
}
