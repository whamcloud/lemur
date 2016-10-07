// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package harness

import (
	"os/exec"
	"regexp"

	"github.com/pkg/errors"

	"github.com/intel-hpdd/logging/debug"
)

// LfsDriver returns an instance of the lfs driver
func LfsDriver() HsmDriver {
	return &lfsDriver{}
}

type lfsDriver struct {
	binPath string
}

func (lfs *lfsDriver) run(args ...string) ([]byte, error) {
	if lfs.binPath == "" {
		var err error
		if lfs.binPath, err = exec.LookPath("lfs"); err != nil {
			return nil, errors.Wrap(err, "Unable to find lfs binary")
		}
	}

	// TODO: Capture stdout/err
	// TODO: run with timeout
	return exec.Command(lfs.binPath, args...).Output() // #nosec
}

func (lfs *lfsDriver) GetState(filePath string) (HsmState, error) {
	out, err := lfs.run("hsm_state", filePath)
	if err != nil {
		return HsmUnknown, errors.Wrapf(err, "Failed to get hsm_state: %s", err.(*exec.ExitError).Stderr)
	}

	stateRe := regexp.MustCompile(`^([^:]+):\s+\(\w+\)(?:\s([\w\s]+))?,?.*`)
	matches := stateRe.FindSubmatch(out)
	debug.Printf("matches (%d): %s", len(matches), matches)
	if len(matches) != 3 {
		return HsmUnknown, errors.Errorf("Unable to parse status from %q", out)
	}

	switch string(matches[2]) {
	case "":
		return HsmUnmanaged, nil
	case "exists":
		return HsmUnarchived, nil
	case "exists archived":
		return HsmArchived, nil
	case "released exists archived":
		return HsmReleased, nil
	default:
		return HsmUnknown, errors.Errorf("Unknown state: %s", matches[2])
	}
}

func (lfs *lfsDriver) Archive(filePath string) error {
	_, err := lfs.run("hsm_archive", "--archive", "1", filePath)
	return err
}

func (lfs *lfsDriver) Restore(filePath string) error {
	_, err := lfs.run("hsm_restore", filePath)
	return err
}

func (lfs *lfsDriver) Remove(filePath string) error {
	_, err := lfs.run("hsm_remove", filePath)
	return err
}

func (lfs *lfsDriver) Release(filePath string) error {
	_, err := lfs.run("hsm_release", filePath)
	return err
}
