package agent_test

import (
	"testing"

	"github.intel.com/hpdd/lustre/hsm"
	"github.intel.com/hpdd/policy/pdm/lhsmd/agent"
	_ "github.intel.com/hpdd/policy/pdm/lhsmd/transport/grpc"

	"golang.org/x/net/context"
)

func TestAgentStartStop(t *testing.T) {
	cfg := agent.DefaultConfig()
	cfg.Transport.Port = 12345
	mon := agent.NewMonitor()
	as := hsm.NewTestSource()
	ep := agent.NewEndpoints()
	ta := agent.NewTestAgent(t, cfg, mon, as, ep)

	if err := ta.Start(context.Background()); err != nil {
		t.Fatalf("Test agent startup failed: %s", err)
	}

	// Wait for the agent to signal that it has started
	<-ta.Started()

	ta.Stop()
}
