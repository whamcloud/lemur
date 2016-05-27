package uat

import (
	"github.com/DATA-DOG/godog"
	"github.com/pkg/errors"
	"gopkg.in/cucumber/gherkin-go.v3"

	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/policy/pdm/uat/harness"
	"github.intel.com/hpdd/policy/pdm/uat/steps"
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
	cfg, err := harness.LoadConfig()
	if err != nil {
		alert.Abort(errors.Wrap(err, "Failed to load test config"))
	}

	// This is a pretty awkward solution, but it should keep us
	// moving forward. We need some place to stash state during
	// a scenario execution. The godog examples hang the step
	// implementations off of a single *suiteContext which gets
	// reset on every scenario, but then we'd lose the ability to
	// define step implementations independently.
	var ctx *harness.ScenarioContext

	// Reset the scenario context before each scenario.
	s.BeforeScenario(func(interface{}) {
		ctx = harness.NewScenarioContext(cfg)
		steps.RegisterContext(ctx)
	})

	// If a step fails, we need to mark the context as failed so
	// that cleanup does the right thing.
	s.AfterStep(func(step *gherkin.Step, err error) {
		if err != nil {
			ctx.Fail()
		}
	})

	// Clean up after the scenario. Anything which needs to be cleaned up
	// should have been registered as a cleanup handler.
	s.AfterScenario(func(i interface{}, err error) {
		// The agent should always be stopped.
		if err := harness.StopAgent(ctx); err != nil {
			alert.Warnf("Failed to stop agent after scenario: %s", err)
			ctx.Fail()
		}

		if ctx.Failed() && !cfg.CleanupOnFailure {
			alert.Warnf("Scenario failed and CleanupOnFailure is not set. Not cleaning up %s and other temporary files.", ctx.Workdir())
			return
		}

		if err := ctx.Cleanup(); err != nil {
			alert.Warnf("Error during post-scenario cleanup: %s", err)
		}
	})

	// Register steps with the suite runner.
	for matcher, step := range steps.WithMatchers {
		s.Step(matcher, step)
	}
}
