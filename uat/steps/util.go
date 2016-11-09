// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package steps

import (
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/intel-hpdd/logging/debug"

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

type (
	unixProcess struct {
		Pid        int
		Executable string
	}
)

func newUnixProcess(pid int) (*unixProcess, error) {
	p := &unixProcess{Pid: pid}
	return p, p.GetExe()
}

func (p *unixProcess) GetExe() error {
	fullPath, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", p.Pid))
	p.Executable = fullPath

	return err
}

func lsProcs() ([]*unixProcess, error) {
	d, err := os.Open("/proc")
	if err != nil {
		return nil, errors.Wrap(err, "Unable to open /proc")
	}
	defer d.Close()

	// preallocate a bit
	procs := make([]*unixProcess, 0, 100)
	for {
		entries, err := d.Readdir(25)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, errors.Wrap(err, "Error while scanning /proc")
		}

		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}

			// Skip non-PID files
			name := entry.Name()
			if name[0] < '0' || name[0] > '9' {
				continue
			}

			pid, err := strconv.ParseInt(name, 10, 0)
			if err != nil {
				return nil, errors.Wrapf(err, "Couldn't parse %s into a pid", name)
			}

			// At this point, an error is probably due to trying
			// to look at a process that has already exited by
			// now, so ignore it.
			p, err := newUnixProcess(int(pid))
			if err != nil {
				continue
			}

			procs = append(procs, p)
		}
	}

	return procs, nil
}

func findProcess(psName string) (*unixProcess, error) {
	psList, err := lsProcs()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to get process list")
	}

	for _, process := range psList {
		if path.Base(process.Executable) == psName {
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
			if p.Pid != pid {
				return errors.Errorf("Found %s, but pid %d does not match expected pid %d!", psName, p.Pid, pid)
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
		duration := 100 * time.Millisecond
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
					timeout,
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
