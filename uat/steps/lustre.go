// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package steps

import (
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"github.com/intel-hpdd/logging/alert"
	"github.com/intel-hpdd/go-lustre/fs"
	"github.com/intel-hpdd/go-lustre/pkg/mntent"
)

func init() {
	addStep(`^I have a Lustre filesystem$`, iHaveALustreFilesystem)
	addStep(`^the HSM coordinator is (enabled|disabled)$`, hsmIsInState)
}

func hsmIsInState(expected string) error {
	// We've established that only the first MDT acts as the HSM
	// controller for a fs, right?
	hsmControls, err := filepath.Glob("/proc/fs/lustre/mdt/*-MDT0000/hsm_control")
	if err != nil {
		return errors.Wrap(err, "Failed to Glob() HSM control file")
	}
	if len(hsmControls) == 0 {
		alert.Warn("No MDT found on this system; can't verify coordinator state")
		return nil
	}
	if len(hsmControls) > 1 {
		return errors.Errorf("Expected exactly 1 MDT (found %d)", len(hsmControls))
	}

	buf, err := ioutil.ReadFile(hsmControls[0])
	if err != nil {
		return errors.Wrapf(err, "Failed to read %s", hsmControls[0])
	}

	actual := strings.TrimSpace(string(buf))
	if actual != expected {
		return errors.Errorf("Coordinator state is %s, but expected %s", actual, expected)
	}

	return nil
}

func iHaveALustreFilesystem() error {
	if ctx.Config.LustrePath != "" {
		if _, err := fs.MountRoot(ctx.Config.LustrePath); err != nil {
			return errors.Errorf("Configured Lustre path is invalid: %s", err)
		}
		return nil
	}

	entries, err := mntent.GetEntriesByType("lustre")
	if err != nil {
		return errors.Wrap(err, "Failed to get Lustre mounts")
	}

	for _, entry := range entries {
		if _, err := fs.MountRoot(entry.Dir); err == nil {
			ctx.Config.LustrePath = entry.Dir
			return nil
		}
	}

	return errors.Errorf("No Lustre filesystem found")
}
