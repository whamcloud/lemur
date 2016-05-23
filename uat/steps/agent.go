package steps

import (
	"fmt"

	"github.intel.com/hpdd/policy/pdm/uat/harness"
)

func init() {
	addStep(`^I (configure|start|stop) the HSM Agent$`, iControlTheHSMAgent)
	addStep(`^the HSM Agent should be (running|stopped)$`, theHSMAgentShouldBe)
}

func iControlTheHSMAgent(action string) error {
	switch action {
	case "configure":
		return harness.ConfigureAgent(ctx)
	case "start":
		return harness.StartAgent(ctx)
	case "stop":
		return harness.StopAgent(ctx)
	default:
		return fmt.Errorf("Unknown agent action %q", action)
	}
}

func theHSMAgentShouldBe(state string) error {
	agentInDesiredState := func() error {
		return checkProcessState(harness.HsmAgentBinary, state)
	}
	return waitFor(agentInDesiredState, DefaultTimeout)
}
