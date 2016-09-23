package agent_test

import (
	"testing"
	"time"

	"github.intel.com/hpdd/lemur/cmd/lhsmd/agent"
	_ "github.intel.com/hpdd/lemur/cmd/lhsmd/transport/grpc"
	"github.intel.com/hpdd/lemur/pkg/fsroot"
	"github.intel.com/hpdd/lustre/hsm"

	"golang.org/x/net/context"
)

func TestAgentStartStop(t *testing.T) {
	cfg := agent.DefaultConfig()
	cfg.Transport.SocketDir = "/tmp"
	as := hsm.NewTestSource()
	ta, err := agent.New(cfg, fsroot.Test(cfg.AgentMountpoint()), as)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		if err := ta.Start(context.Background()); err != nil {
			t.Fatalf("Test agent startup failed: %s", err)
		}
	}()

	// Wait for the agent to signal that it has started
	ta.StartWaitFor(5 * time.Second)

	ta.Stop()
}
