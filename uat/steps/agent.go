// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package steps

import (
	"fmt"

	"github.com/intel-hpdd/lemur/uat/harness"
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
		pid, err := ctx.AgentDriver.AgentPid()
		if err != nil {
			return err
		}
		return checkProcessState(harness.HsmAgentBinary, state, pid)
	}
	return waitFor(agentInDesiredState, DefaultTimeout)
}
