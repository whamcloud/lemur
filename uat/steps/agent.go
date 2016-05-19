package steps

import (
	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/policy/pdm/uat/drivers"

	"github.com/pkg/errors"
)

func init() {
	addStep(`^I start the HSM Agent$`, Context.iStartTheHSMAgent)
	addStep(`^the HSM Agent should be (running|stopped)$`, Context.theHSMAgentShouldBe)
}

func (sc *stepContext) iStartTheHSMAgent() error {
	cfgPath, err := sc.AgentDriver.WriteAgentConfig()
	if err != nil {
		return errors.Wrap(err, "Failed to write test agent config")
	}
	debug.Printf("Wrote agent config to %s", cfgPath)

	return sc.AgentDriver.StartAgent(cfgPath)
}

func (sc *stepContext) theHSMAgentShouldBe(state string) error {
	agentStatus := func() error {
		return checkProcessState(drivers.HsmAgentBinary, state)
	}
	return waitFor(agentStatus, DefaultTimeout)
}
