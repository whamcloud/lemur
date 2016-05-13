package agent_e2e_test

import (
	"flag"
	"os"
	"testing"

	"github.com/fortytw2/leaktest"

	"golang.org/x/net/context"

	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/lustre"
	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/hsm"
	"github.intel.com/hpdd/lustre/llapi"
	"github.intel.com/hpdd/policy/pdm/dmplugin"
	"github.intel.com/hpdd/policy/pdm/lhsmd/agent"
	"github.intel.com/hpdd/policy/pdm/lhsmd/agent/fileid"
	"github.intel.com/hpdd/policy/pdm/lhsmd/config"
	_ "github.intel.com/hpdd/policy/pdm/lhsmd/transport/grpc"
)

var (
	testRpcPort    = 12345
	testArchiveID  = 1
	enableLeakTest = false
)

func init() {
	flag.BoolVar(&enableLeakTest, "leak", false, "enable leak check")
	flag.Var(debug.FlagVar())
	flag.Parse()

	// swap in the dummy implementation
	fileid.EnableTestMode()
}

type (
	signalChan chan struct{}

	testMover struct {
		started        signalChan
		receivedAction chan dmplugin.Action
	}
)

func (t *testMover) Archive(a dmplugin.Action) error {
	debug.Printf("testMover received Archive action: %s", a)
	t.receivedAction <- a
	close(t.receivedAction)

	a.Update(0, 1, 2)
	return nil
}

func (t *testMover) Restore(a dmplugin.Action) error {
	debug.Printf("testMover received Restore action: %s", a)
	t.receivedAction <- a
	close(t.receivedAction)

	a.Update(0, 0, 0)
	return nil
}

func (t *testMover) Remove(a dmplugin.Action) error {
	debug.Printf("testMover received Remove action: %s", a)
	t.receivedAction <- a
	close(t.receivedAction)

	return nil
}

func (t *testMover) Started() signalChan {
	return t.started
}

func (t *testMover) Start() {
	close(t.started)
}

func (t *testMover) ReceivedAction() chan dmplugin.Action {
	return t.receivedAction
}

func newTestMover() *testMover {
	return &testMover{
		started:        make(signalChan),
		receivedAction: make(chan dmplugin.Action),
	}
}

func newTestPlugin(t *testing.T) (dmplugin.Plugin, *testMover) {
	// Not crazy about this, because it means dmplugin.New() isn't covered
	plugin := dmplugin.NewTestPlugin(t, "fake-test-plugin")

	tm := newTestMover()
	plugin.AddMover(&dmplugin.Config{
		Mover:     tm,
		ArchiveID: uint32(testArchiveID),
	})

	return plugin, tm
}

func newTestAgent(t *testing.T) *agent.TestAgent {
	// Ambivalent about doing this config here vs. in agent.TestAgent;
	// leaving it here for now with the idea that tests may want to
	// supply their own implementations of these things.
	cfg := agent.DefaultConfig()
	cfg.Transport.Port = testRpcPort
	// little hack, to allow testing in parallel
	testRpcPort++

	mon := agent.NewMonitor()
	as := hsm.NewTestSource()
	ep := agent.NewEndpoints()

	// Configure environment to launch plugins
	os.Setenv(config.AgentConnEnvVar, cfg.Transport.ConnectionString())
	os.Setenv(config.PluginMountpointEnvVar, "/tmp")
	os.Setenv(config.ConfigDirEnvVar, "/tmp")

	return agent.NewTestAgent(t, cfg, mon, as, ep)
}

func TestArchiveEndToEnd(t *testing.T) {
	// NB: Leaktest finds a leak in the go-metrics library, but everything
	// else seems fine.
	if enableLeakTest {
		defer leaktest.Check(t)()
	}

	// First, start a test agent to delegate work to test data movers.
	ta := newTestAgent(t)
	if err := ta.Start(context.Background()); err != nil {
		t.Fatalf("Test agent startup failed: %s", err)
	}
	defer ta.Stop()

	// Wait for the agent to signal that it has started
	<-ta.Started()

	// Now, start a data mover plugin which will connect to our
	// test agent to receive an injected action.
	p, tm := newTestPlugin(t)
	ta.AddPlugin(p)

	// Wait for the mover to signal that it has been started
	<-tm.Started()

	testFid, err := lustre.ParseFid("0xdead:0xbeef:0x0")
	if err != nil {
		t.Fatalf("error generating test fid: %s", err)
	}
	// Inject an action
	tr := hsm.NewTestRequest(uint(testArchiveID), llapi.HsmActionArchive, testFid)
	ta.AddAction(tr)

	// Wait for the mover to signal that it has received the action
	// on the other side of the RPC interface
	action := <-tm.ReceivedAction()

	actionPath := action.PrimaryPath()
	fidPath := fs.FidRelativePath(testFid)
	if actionPath != fidPath {
		t.Fatalf("expected path %s, got %s", fidPath, actionPath)
	}

	// Wait for the mover to send a progress update on the action
	update := <-tr.ProgressUpdates()
	debug.Printf("Update: %s", update)

	// Wait for the mover to end the request
	<-tr.Finished()
}

func TestRestoreEndToEnd(t *testing.T) {
	if enableLeakTest {
		defer leaktest.Check(t)()
	}

	// First, start a test agent to delegate work to test data movers.
	ta := newTestAgent(t)
	if err := ta.Start(context.Background()); err != nil {
		t.Fatalf("Test agent startup failed: %s", err)
	}
	defer ta.Stop()

	// Wait for the agent to signal that it has started
	<-ta.Started()

	// Now, start a data mover plugin which will connect to our
	// test agent to receive an injected action.
	p, tm := newTestPlugin(t)
	ta.AddPlugin(p)

	// Wait for the mover to signal that it has been started
	<-tm.Started()

	testFid, err := lustre.ParseFid("0xdead:0xbeef:0x0")
	if err != nil {
		t.Fatalf("error generating test fid: %s", err)
	}
	fileid.Set(fs.FidRelativePath(testFid), []byte("moo"))
	// Inject an action
	tr := hsm.NewTestRequest(uint(testArchiveID), llapi.HsmActionRestore, testFid)
	ta.AddAction(tr)

	// Wait for the mover to signal that it has received the action
	// on the other side of the RPC interface
	action := <-tm.ReceivedAction()

	actionPath := action.PrimaryPath()
	fidPath := fs.FidRelativePath(testFid)
	if actionPath != fidPath {
		t.Fatalf("expected path %s, got %s", fidPath, actionPath)
	}

	// Wait for the mover to send a progress update on the action
	update := <-tr.ProgressUpdates()
	debug.Printf("Update: %s", update)

	// Wait for the mover to end the request
	<-tr.Finished()
}

func TestRemoveEndToEnd(t *testing.T) {
	if enableLeakTest {
		defer leaktest.Check(t)()
	}

	// First, start a test agent to delegate work to test data movers.
	ta := newTestAgent(t)
	if err := ta.Start(context.Background()); err != nil {
		t.Fatalf("Test agent startup failed: %s", err)
	}
	defer ta.Stop()

	// Wait for the agent to signal that it has started
	<-ta.Started()

	// Now, start a data mover plugin which will connect to our
	// test agent to receive an injected action.
	p, tm := newTestPlugin(t)
	ta.AddPlugin(p)

	// Wait for the mover to signal that it has been started
	<-tm.Started()

	testFid, err := lustre.ParseFid("0xdead:0xbeef:0x0")
	if err != nil {
		t.Fatalf("error generating test fid: %s", err)
	}
	fileid.Set(fs.FidRelativePath(testFid), []byte("moo"))
	// Inject an action
	tr := hsm.NewTestRequest(uint(testArchiveID), llapi.HsmActionRemove, testFid)
	ta.AddAction(tr)

	// Wait for the mover to signal that it has received the action
	// on the other side of the RPC interface
	action := <-tm.ReceivedAction()

	actionPath := action.PrimaryPath()
	fidPath := fs.FidRelativePath(testFid)
	if actionPath != fidPath {
		t.Fatalf("expected path %s, got %s", fidPath, actionPath)
	}

	// Wait for the mover to end the request
	<-tr.Finished()
}
