package agent_test

import (
	"testing"

	"github.intel.com/hpdd/lemur/cmd/lhsmd/agent"
	_ "github.intel.com/hpdd/lemur/cmd/lhsmd/transport/grpc"
	"github.intel.com/hpdd/lustre/hsm"

	"golang.org/x/net/context"
)

func TestAgentStartStop(t *testing.T) {
	cfg := agent.DefaultConfig()
	cfg.Transport.SocketDir = "/tmp"
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
