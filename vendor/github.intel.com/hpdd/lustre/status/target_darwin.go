package status

import "github.intel.com/hpdd/lustre"

// LovName returns the uniqe name for the LOV devcie for the client associated with the path.
func LovName(p string) (string, error) {
	return "", lustre.ErrUnimplemented
}

// LmvName returns the uniqe name for the LMV device for the client associated with the path.
func LmvName(p string) (string, error) {
	return "", lustre.ErrUnimplemented
}
