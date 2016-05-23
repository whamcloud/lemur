package steps

import (
	"time"

	"github.intel.com/hpdd/policy/pdm/uat/harness"
)

func init() {
	addStep(`^I configure the (posix|s3) data mover$`, iConfigureADataMover)
	addStep(`^the (posix|s3) data mover should be (running|stopped)$`, theDataMoverShouldBe)
}

func iConfigureADataMover(dmType string) error {
	return harness.AddConfiguredMover(ctx, harness.HsmPluginPrefix+dmType)
}

func theDataMoverShouldBe(dmType, state string) error {
	// Ick. I /detest/ sleeps in test code, as they are typically
	// a really crappy way to work around races. In this case,
	// however, we really need to wait for the plugin to start
	// and register before we proceed. Otherwise an action may
	// come in before any handlers are registered in the agent,
	// and the action will be discarded. We might want to revisit
	// that design choice and instead queue actions when we don't
	// have any handlers yet.
	time.Sleep(1 * time.Second)

	dmStatus := func() error {
		return checkProcessState(harness.HsmPluginPrefix+dmType, state)
	}
	return waitFor(dmStatus, DefaultTimeout)
}
