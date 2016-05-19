package uat

import (
	"os"

	"github.com/DATA-DOG/godog"
	"github.com/pkg/errors"

	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/policy/pdm/uat/steps"
	"github.intel.com/hpdd/policy/pdm/uat/suite"
)

// This is the entry point for godog tests. The godog CLI cmd automatically
// identifies it via the *godog.Suite parameter and generates a test
// binary based on this function.
//
// The test binary then scans through features/ to load and execute the
// feature tests it finds there.
//
// Implementation of the feature tests is done in the steps package.
//
// These tests are intended to automate acceptance testing as might
// be manually performed by an end-user (i.e. a filesystem admin and
// their users), or by developers prior to delivery.
//
// These tests SHOULD NOT be seen as a replacement for proper unit
// tests.
//
// Scenarios should always be isolated from one another as much as
// possible. Setting up and tearing down a whole Lustre filesystem
// is not practical, but a scenario should NEVER depend on or be
// affected by the outcome of a prior scenario. Trust me, that way
// ends in tears.
//
// Steps within a scenario may depend on each other, but this should
// be done very judiciously. Troubleshooting scenario failures gets
// complicated very quickly when there is a rat's nest of step
// interdependencies.
func configureSuite(s *godog.Suite) {
	cfg, err := suite.LoadConfig()
	if err != nil {
		alert.Abort(errors.Wrap(err, "Failed to load test config"))
	}
	steps.Context.AddSuiteConfig(cfg)

	// FIXME: This needs work -- it should reset state but not need to
	// reload the suite config.
	//s.BeforeScenario(steps.Context.Reset)
	s.AfterScenario(func(i interface{}, err error) {
		if err != nil {
			errors.Fprint(os.Stderr, errors.Wrap(err, "Scenario failed"))
		}
	})
	// FIXME: This isn't working reliably. Can't tell if it's problems
	// in the agent shutdown or races in the harness.
	s.AfterScenario(func(interface{}, error) {
		if err := steps.Context.StopHsmAgent(); err != nil {
			alert.Warnf("failed to stop agent: %s", err)
		}
	})
	// FIXME: This cleanup needs to happen selectively. When failures
	// occur, the test dir should be left behind.
	/*s.AfterScenario(func(interface{}, error) {
		if err := steps.Context.Cleanup(); err != nil {
			errors.Fprint(os.Stderr, errors.Wrap(err, "Cleanup failed"))
		}
	})*/

	for matcher, step := range steps.WithMatchers {
		s.Step(matcher, step)
	}
}
