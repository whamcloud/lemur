package steps

import (
	"fmt"
	"path"
	"time"

	"github.intel.com/hpdd/logging/debug"

	"github.com/mjmac/go-ps" // until PR is merged (hopefully)
	"github.com/pkg/errors"
)

const (
	// DefaultTimeout is the default waitFor timeout, in seconds
	DefaultTimeout = 10

	// StatusUpdateTimeout is the timeout for a file status update
	StatusUpdateTimeout = DefaultTimeout * 6
)

func findProcess(psName string) (ps.Process, error) {
	psList, err := ps.Processes()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to get process list")
	}

	for _, process := range psList {
		if path.Base(process.Executable()) == psName {
			return process, nil
		}
	}

	return nil, fmt.Errorf("Failed to find %s in running process list", psName)
}

func checkProcessState(psName, state string, pid int) error {
	p, err := findProcess(psName)

	switch state {
	case "running":
		if err != nil {
			return err
		}
		// Optionally check that the found process matches the
		// expected pid.
		debug.Printf("Checking pid %d of %s", pid, psName)
		if pid > 0 {
			if p.Pid() != pid {
				return fmt.Errorf("Found %s, but pid %d does not match expected pid %d!", psName, p.Pid(), pid)
			}
		}
		return nil
	case "stopped":
		if err == nil {
			return fmt.Errorf("%s is still running", psName)
		}
		return nil
	default:
		return fmt.Errorf("Unknown state: %s", state)
	}
}

func waitFor(waitFn func() error, timeout int) error {
	success := make(chan struct{})
	go func() {
		for {
			if err := waitFn(); err == nil {
				close(success)
				return
			}
		}
	}()

	for {
		select {
		case <-success:
			return nil
		case <-time.After(time.Duration(timeout) * time.Second):
			return fmt.Errorf("Timed out waiting for result")
		}
	}
}
