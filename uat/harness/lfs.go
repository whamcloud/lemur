package harness

import (
	"fmt"
	"os/exec"
	"regexp"

	"github.com/pkg/errors"

	"github.intel.com/hpdd/logging/debug"
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
	return exec.Command(lfs.binPath, args...).Output()
}

func (lfs *lfsDriver) GetState(filePath string) (HsmState, error) {
	out, err := lfs.run("hsm_state", filePath)
	if err != nil {
		return HsmUnknown, errors.Wrapf(err, "Failed to get hsm_state: %s", err.(*exec.ExitError).Stderr)
	}

	stateRe := regexp.MustCompile(`^([^:]+):\s+\(\w+\)\s([\w\s]+),.*`)
	matches := stateRe.FindSubmatch(out)
	debug.Printf("matches (%d): %s", len(matches), matches)
	if len(matches) != 3 {
		return HsmUnknown, fmt.Errorf("Unable to parse status from %s", out)
	}

	switch string(matches[2]) {
	case "exists":
		return HsmUnarchived, nil
	case "exists archived":
		return HsmArchived, nil
	default:
		return HsmUnknown, fmt.Errorf("Unknown state: %s", matches[2])
	}
}

func (lfs *lfsDriver) Archive(filePath string) error {
	_, err := lfs.run("hsm_archive", "--archive", "1", filePath)
	return err
}

func (lfs *lfsDriver) Restore(filePath string) error {
	return nil
}

func (lfs *lfsDriver) Remove(filePath string) error {
	return nil
}

func (lfs *lfsDriver) Release(filePath string) error {
	return nil
}
