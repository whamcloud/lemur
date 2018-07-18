// Copyright (c) 2016 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package hsm

import (
	"github.com/intel-hpdd/go-lustre"
	"github.com/intel-hpdd/go-lustre/fs"
	"github.com/intel-hpdd/go-lustre/llapi"
)

// RequestArchive submits a request to the coordinator for the
// specified list of fids to be archived to the specfied archive id.
func RequestArchive(root fs.RootDir, archiveID uint, fids []*lustre.Fid) error {
	return hsmRequest(root, llapi.HsmUserArchive, archiveID, fids)
}

// RequestRestore submits a request to the coordinator for the
// specified list of fids to be restored from the specfied archive id.
func RequestRestore(root fs.RootDir, archiveID uint, fids []*lustre.Fid) error {
	return hsmRequest(root, llapi.HsmUserRestore, archiveID, fids)
}

// RequestRelease submits a request to the coordinator for the
// specified list of fids to be released.
func RequestRelease(root fs.RootDir, archiveID uint, fids []*lustre.Fid) error {
	return hsmRequest(root, llapi.HsmUserRelease, archiveID, fids)
}

// RequestRemove submits a request to the coordinator for the
// specified list of fids to be removed from the HSM backend.
func RequestRemove(root fs.RootDir, archiveID uint, fids []*lustre.Fid) error {
	return hsmRequest(root, llapi.HsmUserRemove, archiveID, fids)
}

// RequestCancel submits a request to the coordinator to cancel any
// outstanding requests involving the specified list of fids.
func RequestCancel(root fs.RootDir, archiveID uint, fids []*lustre.Fid) error {
	return hsmRequest(root, llapi.HsmUserCancel, archiveID, fids)
}

func hsmRequest(root fs.RootDir, cmd llapi.HsmUserAction, archiveID uint, fids []*lustre.Fid) error {
	if _, err := llapi.HsmRequest(root.Path(), cmd, archiveID, fids); err != nil {
		return err
	}
	return nil
}
