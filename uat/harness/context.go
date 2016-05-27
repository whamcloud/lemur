package harness

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"sync"

	"github.intel.com/hpdd/logging/debug"
)

type (
	cleanupFn func() error

	kvmap map[string]string

	// ScenarioContext holds per-scenario context. It should be unique for
	// each scenario, in order to avoid leaking state between scenarios.
	ScenarioContext struct {
		sync.Mutex

		HsmDriver   HsmDriver
		AgentDriver *AgentDriver
		Config      *Config
		TestFiles   map[string]*TestFile

		// These are per-scenario, unless otherwise configured
		S3Bucket string
		S3Prefix string

		cleanupFunctions []cleanupFn
		workdir          string
		kv               kvmap
		failed           bool

		setup *sync.Once
	}

	multiError []error
)

func (m multiError) Error() string {
	var buf bytes.Buffer

	fmt.Fprintf(&buf, "Error(s):\n")
	for _, err := range m {
		fmt.Fprintf(&buf, "  %s", err)
	}

	return buf.String()
}

// Fail marks the scenario as failed
func (s *ScenarioContext) Fail() {
	s.Lock()
	defer s.Unlock()

	s.failed = true
}

// Failed returns the protected value
func (s *ScenarioContext) Failed() bool {
	s.Lock()
	defer s.Unlock()

	return s.failed
}

// SetKey inserts or updates a value for a given key
func (s *ScenarioContext) SetKey(key, value string) {
	s.Lock()
	defer s.Unlock()
	s.kv[key] = value
}

// GetKey attempts to get the value associated with key, or fails
func (s *ScenarioContext) GetKey(key string) (string, error) {
	s.Lock()
	defer s.Unlock()
	val, ok := s.kv[key]
	if !ok {
		return "", fmt.Errorf("No value for key %s found", key)
	}

	return val, nil
}

func (s *ScenarioContext) createWorkdir() {
	var err error
	s.workdir, err = ioutil.TempDir("", "lhsmd-uat")
	if err != nil {
		panic(fmt.Errorf("failed to create workdir: %s", err))
	}

	/*
		testDirs := []string{"/etc/lhsmd", "/archives/1"}
		for _, td := range testDirs {
			if err := os.MkdirAll(wd+td, 0755); err != nil {
				panic(fmt.Errorf("failed to mkdir %s: %s", wd+td, err))
			}
		}
	*/

	s.AddCleanup(func() error {
		debug.Printf("Cleaning up %s", s.workdir)
		return os.RemoveAll(s.workdir)
	})
}

// Workdir returns the path to the context's working directory, which
// is created as a tempdir.
func (s *ScenarioContext) Workdir() string {
	s.setup.Do(func() {
		s.createWorkdir()
	})

	return s.workdir
}

// Cleanup runs all cleanup functions, and returns an error if any of them fail
func (s *ScenarioContext) Cleanup() error {
	var errors multiError

	for _, fn := range s.cleanupFunctions {
		if err := fn(); err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return errors
	}
	return nil
}

// AddCleanup registers a cleanup handler
func (s *ScenarioContext) AddCleanup(fn cleanupFn) {
	s.cleanupFunctions = append(s.cleanupFunctions, fn)
}

// NewScenarioContext returns a freshly-initialized *ScenarioContext
func NewScenarioContext(cfg *Config) *ScenarioContext {
	hsmDriver, err := getHsmDriver(cfg)
	if err != nil {
		panic("Unable to find a suitable HSM driver")
	}

	return &ScenarioContext{
		Config:    cfg,
		HsmDriver: hsmDriver,
		TestFiles: make(map[string]*TestFile),

		kv:    make(kvmap),
		setup: &sync.Once{},
	}
}
