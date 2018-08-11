// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package fsroot

import "golang.org/x/sys/unix"

func getFsID(mountPath string) (*FsID, error) {
	statfs := &unix.Statfs_t{}

	if err := unix.Statfs(mountPath, statfs); err != nil {
		return nil, err
	}
	var id FsID
	id.val[0] = statfs.Fsid.Val[0]
	id.val[1] = statfs.Fsid.Val[1]
	return &id, nil
}
