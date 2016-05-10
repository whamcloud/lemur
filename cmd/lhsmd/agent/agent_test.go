package agent_test

import (
	"flag"
	"fmt"
	"os"
	"testing"

	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/lustre"
	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/hsm"
	"github.intel.com/hpdd/lustre/llapi"
	"github.intel.com/hpdd/policy/pdm/dmplugin"
	"github.intel.com/hpdd/policy/pdm/lhsmd/agent"
	"github.intel.com/hpdd/policy/pdm/lhsmd/config"
	_ "github.intel.com/hpdd/policy/pdm/lhsmd/transport/grpc"

	"golang.org/x/net/context"
)

var testConfig string

func init() {
	flag.StringVar(&testConfig, "tc", "", "path to test agent config")
	flag.Parse()
}

func LoadTestConfig() (*agent.Config, error) {
	if testConfig == "" {
		return nil, fmt.Errorf("Unable to load test agent config file")
	}

	return agent.LoadConfig(testConfig)
}

func TestAgentStartStop(t *testing.T) {
	cfg := agent.DefaultConfig()
	cfg.Transport.Port = 12345
	mon := agent.NewMonitor()
	as := hsm.NewTestSource()
	ep := agent.NewEndpoints()
	ta := agent.TestAgent(cfg, mon, as, ep)

	started := make(chan struct{})
	var startFunc = func() {
		close(started)
	}
	ctx := context.WithValue(context.Background(), "startSignal", startFunc)
	errChan := make(chan error)
	// Start the agent in its own goroutine to avoid blocking the test
	go func() {
		errChan <- ta.Start(ctx)
	}()

	// Wait for the TestSource to signal that it has started
	<-started
	ta.Stop()
	if err := <-errChan; err != nil {
		t.Fatalf("error starting agent: %s", err)
	}
}

func TestAgentPlugin(t *testing.T) {
	// NB: Leaktest finds a leak in the go-metrics library, but everything
	// else seems fine.
	//defer leaktest.Check(t)()

	// First, start a test agent to delegate work to test data movers.

	cfg := agent.DefaultConfig()
	cfg.Transport.Port = 12345
	mon := agent.NewMonitor()
	as := hsm.NewTestSource()
	ep := agent.NewEndpoints()
	ta := agent.TestAgent(cfg, mon, as, ep)

	agentStarted := make(chan struct{})
	var agentStartFn = func() {
		close(agentStarted)
	}
	ctx := context.WithValue(context.Background(), "startSignal", agentStartFn)
	// Start the agent in its own goroutine to avoid blocking the test
	go func() {
		if err := ta.Start(ctx); err != nil {
			t.Errorf("test agent failed to start: %s", err)
		}
	}()

	// Wait for the TestSource to signal that it has started
	<-agentStarted

	// Now, start a data mover plugin which will connect to our
	// test agent to receive an injected action.

	// Configure environment to launch plugin
	os.Setenv(config.AgentConnEnvVar, cfg.Transport.ConnectionString())
	os.Setenv(config.PluginMountpointEnvVar, "/tmp")
	os.Setenv(config.ConfigDirEnvVar, "/tmp")
	// Not crazy about this, because it means dmplugin.New() isn't covered
	plugin := dmplugin.NewTestPlugin(t, "fake-test-plugin")

	moverStarted := make(chan struct{})
	var moverStartedFn = func() {
		close(moverStarted)
	}
	messageReceived := make(chan struct{})
	var messageReceivedFn = func() {
		close(messageReceived)
	}
	mover := &testMover{
		startedFunc:  moverStartedFn,
		receivedFunc: messageReceivedFn,
	}
	plugin.AddMover(&dmplugin.Config{
		Mover:     mover,
		ArchiveID: 1,
	})

	// Wait for the mover to signal that it has been started
	<-moverStarted

	testFid, err := lustre.ParseFid("0xdead:0xbeef:0x0")
	if err != nil {
		t.Fatalf("error generating test fid: %s", err)
	}
	// Inject an action
	as.AddAction(hsm.NewTestRequest(1, llapi.HsmActionArchive, testFid))

	// Wait for the mover to signal that it has received the action
	// on the other side of the RPC interface
	<-messageReceived

	actionPath := mover.receivedArchiveAction.PrimaryPath()
	fidPath := fs.FidRelativePath(testFid)
	if actionPath != fidPath {
		t.Fatalf("expected path %s, got %s", fidPath, actionPath)
	}

	ta.Stop()
}

type testMover struct {
	startedFunc           func()
	receivedFunc          func()
	receivedArchiveAction dmplugin.Action
}

func (t *testMover) Archive(a dmplugin.Action) error {
	debug.Printf("testMover received Archive action: %s", a)
	t.receivedArchiveAction = a
	t.receivedFunc()
	return nil
}

func (t *testMover) Start() {
	t.startedFunc()
}
