package agent

import (
	"fmt"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.intel.com/hpdd/lustre/hsm"
	"github.intel.com/hpdd/policy/pdm/dmplugin"
	"github.intel.com/hpdd/policy/pkg/client"
)

const (
	agentStartTimeout = 5
)

type (
	// TestAgent wraps an HsmAgent and provides extra test-specific methods
	TestAgent struct {
		HsmAgent
		as         *hsm.TestSource
		t          *testing.T
		started    chan struct{}
		startError chan error
		plugins    []dmplugin.Plugin
	}
)

// Agent returns the embedded HsmAgent
func (ta *TestAgent) Agent() *HsmAgent {
	return &ta.HsmAgent
}

// AddAction is used to inject an HSM action into the agent's test
// ActionSource. It is a stand-in for a connection to a real Lustre
// HSM coordinator.
func (ta *TestAgent) AddAction(ar hsm.ActionRequest) {
	ta.as.AddAction(ar)
}

// AddPlugin adds a plugin to be managed by the test agent. Mostly useful
// for ensuring that the plugin is cleanly shut down at test end.
func (ta *TestAgent) AddPlugin(p dmplugin.Plugin) {
	ta.plugins = append(ta.plugins, p)
}

// Started returns a channel the test code can block on to wait for the
// agent to signal that it has started
func (ta *TestAgent) Started() chan struct{} {
	return ta.started
}

// Start calls the embedded HsmAgent's Start() method and then signals
// that the agent has started.
func (ta *TestAgent) Start(ctx context.Context) error {
	// Let's hide some of the kludgy stuff in here. If we come up with
	// a cleaner way of safely starting the agent under test, we can
	// just change this implementation rather than needing to update
	// test code.

	// For now, we need to rely on our TestSource's closing of a channel
	// when it's started up as a proxy for the agent being started. We
	// send in a callback as a value on the context.
	tsStarted := make(chan struct{})
	var tsStartedFn = func() {
		close(tsStarted)
	}
	ctx = context.WithValue(ctx, "startSignal", tsStartedFn)

	go func() {
		// will block unless there was an error
		ta.startError <- ta.HsmAgent.Start(ctx)
	}()

	for {
		select {
		case err := <-ta.startError:
			return err
		case <-tsStarted:
			close(ta.started)
			return nil
		case <-time.After(agentStartTimeout * time.Second):
			return fmt.Errorf("Agent startup timed out after %d seconds", agentStartTimeout)
		}
	}
}

// Stop kills the embedded HSM Agent and any test plugins that were registered
// with the test agent.
func (ta *TestAgent) Stop() {
	for _, p := range ta.plugins {
		p.Stop()
		p.Close()
	}

	ta.HsmAgent.Stop()

	// Wait for HsmAgent.Start() to exit. Any error here is probably
	// pretty weird, but not sure if it's a failure. The main reason
	// we wait for this is to avoid leaking a goroutine on every test
	// which starts a test agent.
	if err := <-ta.startError; err != nil {
		ta.t.Errorf("HsmAgent.Start() returned non-nil error in shutdown: %s", err)
	}
}

// NewTestAgent returns a wrapped *HsmAgent configured for testing
func NewTestAgent(t *testing.T, cfg *Config, mon *PluginMonitor, as *hsm.TestSource, ep *Endpoints) *TestAgent {
	return &TestAgent{
		HsmAgent: HsmAgent{
			stats:        NewActionStats(),
			client:       client.Test(cfg.AgentMountpoint),
			config:       cfg,
			monitor:      mon,
			actionSource: as,
			Endpoints:    ep,
		},
		as:         as, // expose the test implementation
		t:          t,
		startError: make(chan error),
		started:    make(chan struct{}),
	}
}
