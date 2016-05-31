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
	DefaultTimeout = 10 * time.Second

	// StatusUpdateTimeout is the timeout for a file status update
	StatusUpdateTimeout = DefaultTimeout * 6

	// StartupDelay delays checking process status for this amount of
	// time in order to catch processes which fail after starting
	// (e.g. due to bad config, etc)
	StartupDelay = 500 * time.Millisecond
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

	return nil, errors.Errorf("Failed to find %s in running process list", psName)
}

func checkProcessState(psName, state string, pid int) error {
	// Wait a bit before checking, to catch processes which fail
	// after starting.
	time.Sleep(StartupDelay)
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
				return errors.Errorf("Found %s, but pid %d does not match expected pid %d!", psName, p.Pid(), pid)
			}
		}
		return nil
	case "stopped":
		if err == nil {
			return errors.Errorf("%s is still running", psName)
		}
		return nil
	default:
		return errors.Errorf("Unknown state: %s", state)
	}
}

func waitFor(waitFn func() error, timeout time.Duration) error {
	success := make(chan struct{})
	go func() {
		// This will poll with an increasing intervals
		// and then decreasing intervals as the timeout approaches
		duration := time.Duration(100 * time.Millisecond)
		started := time.Now()
		for {
			// prevent this goroutine from running forever
			if time.Since(started) > timeout {
				debug.Printf("waitFor hit timeout: %v > %v", time.Since(started), timeout)
				close(success)
				return
			}

			if err := waitFn(); err == nil {
				close(success)
				return
			}
			// Reduce pause duration if timeout is imminent
			if duration > timeout-time.Since(started) {
				debug.Printf("timeout: %v elapsed: %v reduce timeout from %v to %v",
					time.Duration(timeout),
					time.Since(started),
					duration,
					(timeout-time.Since(started))/2)
				duration = (timeout - time.Since(started)) / 2
			}
			time.Sleep(duration)
			// Allow sleep duration to increase 7-14s, depending on start time
			if duration < 7*time.Second {
				duration *= 2
			}
		}
	}()

	for {
		select {
		case <-success:
			return nil
		case <-time.After(timeout):
			return fmt.Errorf("Timed out waiting for result")
		}
	}
}
