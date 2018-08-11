// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package status

import "github.com/intel-hpdd/go-lustre"

// LovName returns the uniqe name for the LOV devcie for the client associated with the path.
func LovName(p string) (string, error) {
	return "", lustre.ErrUnimplemented
}

// LmvName returns the uniqe name for the LMV device for the client associated with the path.
func LmvName(p string) (string, error) {
	return "", lustre.ErrUnimplemented
}
