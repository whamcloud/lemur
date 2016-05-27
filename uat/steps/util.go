package steps

import (
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

	return nil, errors.Errorf("Failed to find %s in running process list", psName)
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

func waitFor(waitFn func() error, timeout int) error {
	success := make(chan struct{})
	go func() {
		// This will poll with an increasing intervals
		// and then decreasing intervals as the timeout approaches
		duration := time.Duration(100 * time.Millisecond)
		started := time.Now()
		for {
			// prevent this goroutine from running forever
			if time.Since(started) > time.Duration(timeout)*time.Second {
				debug.Printf("waitFor hit timeout: %v > %v", time.Since(started), time.Duration(timeout)*time.Second)
				close(success)
				return
			}

			if err := waitFn(); err == nil {
				close(success)
				return
			}
			// Reduce pause duration if timeout is imminent
			if duration > time.Duration(timeout)*time.Second-time.Since(started) {
				debug.Printf("timeout: %v elapsed: %v reduce timeout from %v to %v",
					time.Duration(time.Duration(timeout)*time.Second),
					time.Since(started),
					duration,
					(time.Duration(timeout)*time.Second-time.Since(started))/2)
				duration = (time.Duration(timeout)*time.Second - time.Since(started)) / 2
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
		case <-time.After(time.Duration(timeout) * time.Second):
			return errors.Errorf("Timed out waiting for result")
		}
	}
}
