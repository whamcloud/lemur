package steps

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/pkg/errors"

	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/policy/pdm/uat/drivers"
	"github.intel.com/hpdd/policy/pdm/uat/suite"
)

// HSMTestFileKey is the prefix/key used to generate and refer to the test file
const HSMTestFileKey = "HSM-test-file"

// WithMatchers holds all registered step matchers and their handlers
var WithMatchers = map[string]HandlerFn{}

// Context provides context for steps
var Context = newContext()

type (
	// HandlerFn is a generated function suitable for godog.Suite.Step()
	HandlerFn interface{}

	stepContext struct {
		SuiteConfig *suite.Config
		HsmDriver   drivers.HsmDriver
		AgentDriver *drivers.AgentDriver

		workdir string
	}
)

func createWorkdir() string {
	wd, err := ioutil.TempDir("", "lhsmd-uat")
	if err != nil {
		panic(fmt.Errorf("failed to create workdir: %s", err))
	}

	testDirs := []string{"/etc/lhsmd", "/archives/1"}
	for _, td := range testDirs {
		if err := os.MkdirAll(wd+td, 0755); err != nil {
			panic(fmt.Errorf("failed to mkdir %s: %s", wd+td, err))
		}
	}

	return wd
}

func newContext() *stepContext {
	c := &stepContext{
		// NB: We may want to have more control over which driver
		// is being used, but for now just find something.
		HsmDriver: drivers.NewMultiHsmDriver(),
		workdir:   createWorkdir(),
	}
	c.AgentDriver = drivers.NewAgentDriver(c.workdir, c.SuiteConfig)

	return c
}

func (sc *stepContext) AddSuiteConfig(cfg *suite.Config) {
	sc.SuiteConfig = cfg
	sc.AgentDriver.AddSuiteConfig(cfg)
}

func (sc *stepContext) Reset(interface{}) {
	debug.Printf("Resetting context")
	cfg := sc.SuiteConfig

	Context = newContext()
	Context.SuiteConfig = cfg
	Context.AgentDriver.AddSuiteConfig(cfg)
}

func (sc *stepContext) Cleanup() error {
	var cleanupErrs error
	if err := os.RemoveAll(sc.workdir); err != nil {
		cleanupErrs = errors.Wrap(err, "Failed to cleanup workdir")
	}
	if testFile, err := sc.SuiteConfig.Get(HSMTestFileKey); err == nil {
		if err := os.Remove(testFile); err != nil {
			cleanupErrs = errors.Wrap(err, "Failed to remove test file")
		}
	}

	return cleanupErrs
}

func addStep(matcher string, handler HandlerFn) {
	WithMatchers[matcher] = handler
}
