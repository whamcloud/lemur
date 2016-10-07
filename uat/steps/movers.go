// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package steps

import (
	"time"

	"github.intel.com/hpdd/lemur/uat/harness"
)

func init() {
	addStep(`^I configure the (posix|s3) data mover$`, iConfigureADataMover)
	addStep(`^the (posix|s3) data mover should be (running|stopped)$`, theDataMoverShouldBe)
}

func iConfigureADataMover(dmType string) error {
	return harness.AddConfiguredMover(ctx, harness.HsmPluginPrefix+dmType+".race")
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
		return checkProcessState(harness.HsmPluginPrefix+dmType+".race", state, -1)
	}
	return waitFor(dmStatus, DefaultTimeout)
}
