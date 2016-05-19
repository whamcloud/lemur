package steps

import (
	"fmt"
	"path"
	"time"

	"github.com/mjmac/go-ps" // until PR is merged (hopefully)
	"github.com/pkg/errors"
)

const (
	// DefaultTimeout is the default waitFor timeout, in seconds
	DefaultTimeout = 10

	// StatusUpdateTimeout is the timeout for a file status update
	StatusUpdateTimeout = DefaultTimeout * 3

	// HsmPluginPrefix is the base name of data mover plugins
	HsmPluginPrefix = "lhsm-plugin-"
)

func findProcess(psName string) (int, error) {
	psList, err := ps.Processes()
	if err != nil {
		return -1, errors.Wrap(err, "Failed to get process list")
	}

	for _, process := range psList {
		if path.Base(process.Executable()) == psName {
			return process.Pid(), nil
		}
	}

	return -1, fmt.Errorf("Failed to find %s in running process list", psName)
}

func checkProcessState(psName, state string) error {
	_, err := findProcess(psName)

	switch state {
	case "running":
		if err != nil {
			return err
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

// Expose some logic here for use in pre/post configuration

// StopHsmAgent kills any running HSM Agent
func (sc *stepContext) StopHsmAgent() error {
	if err := sc.AgentDriver.StopAgent(); err != nil {
		return err
	}

	// clever(?) use of the step definition to wait for the agent to die
	waitForAgentToBe := Context.theHSMAgentShouldBe
	return waitForAgentToBe("stopped")
}
