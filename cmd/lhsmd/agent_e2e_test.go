package main_test

import (
	"flag"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"

	"golang.org/x/net/context"

	"github.intel.com/hpdd/lemur/cmd/lhsmd/agent"
	"github.intel.com/hpdd/lemur/cmd/lhsmd/agent/fileid"
	"github.intel.com/hpdd/lemur/cmd/lhsmd/config"
	_ "github.intel.com/hpdd/lemur/cmd/lhsmd/transport/grpc"
	"github.intel.com/hpdd/lemur/dmplugin"
	"github.intel.com/hpdd/lemur/pkg/fsroot"
	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/lustre"
	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/hsm"
	"github.intel.com/hpdd/lustre/llapi"
)

const (
	testSocketDir = "/tmp"
	testArchiveID = 1
	testReps      = 10
)

var (
	enableLeakTest = false
)

func init() {
	flag.BoolVar(&enableLeakTest, "leak", false, "enable leak check")
	flag.Parse()
	// swap in the dummy implementation
	fileid.EnableTestMode()
}

type (
	signalChan chan struct{}

	testMover struct {
		started        signalChan
		receivedAction chan dmplugin.Action
		plugin         *dmplugin.Plugin
	}
)

func (t *testMover) Archive(a dmplugin.Action) error {
	debug.Printf("testMover received Archive action: %s", a)
	t.receivedAction <- a

	a.Update(0, 1, 2)
	return nil
}

func (t *testMover) Restore(a dmplugin.Action) error {
	debug.Printf("testMover received Restore action: %s", a)
	t.receivedAction <- a

	a.Update(0, 0, 0)
	return nil
}

func (t *testMover) Remove(a dmplugin.Action) error {
	debug.Printf("testMover received Remove action: %s", a)
	t.receivedAction <- a

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

func (t *testMover) Stop() {
	t.plugin.Stop()
	t.plugin.Close()
	close(t.receivedAction)
}

func newTestMover(p *dmplugin.Plugin) *testMover {
	return &testMover{
		started:        make(signalChan),
		receivedAction: make(chan dmplugin.Action),
		plugin:         p,
	}
}

func testStartMover(t *testing.T) *testMover {
	plugin, err := dmplugin.New(path.Base(os.Args[0]), func(path string) (fsroot.Client, error) {
		return fsroot.Test(path), nil
	})
	if err != nil {
		t.Fatal(err)
	}

	tm := newTestMover(plugin)
	plugin.AddMover(&dmplugin.Config{
		Mover:     tm,
		ArchiveID: uint32(testArchiveID),
	})
	go plugin.Run()

	// Wait for the mover to signal that it has been started
	<-tm.Started()

	return tm
}

func newTestAgent(t *testing.T, as hsm.ActionSource) *agent.HsmAgent {
	// Ambivalent about doing this config here vs. in agent.TestAgent;
	// leaving it here for now with the idea that tests may want to
	// supply their own implementations of these things.
	cfg := agent.DefaultConfig()
	cfg.Transport.SocketDir = testSocketDir

	// Configure environment to launch plugins
	os.Setenv(config.AgentConnEnvVar, cfg.Transport.ConnectionString())
	os.Setenv(config.PluginMountpointEnvVar, "/tmp")
	os.Setenv(config.ConfigDirEnvVar, "/tmp")

	a, err := agent.New(cfg, fsroot.Test(cfg.AgentMountpoint()), as)
	if err != nil {
		t.Fatal(err)
	}

	return a
}

func testStartAgent(t *testing.T, as hsm.ActionSource) *agent.HsmAgent {
	ta := newTestAgent(t, as)
	go func() {
		if err := ta.Start(context.Background()); err != nil {
			t.Fatalf("Test agent startup failed: %s", err)
		}
	}()

	// Wait for the agent to signal that it has started
	ta.StartWaitFor(5 * time.Second)

	return ta
}

func testGenFid(t *testing.T, id int) *lustre.Fid {
	testFid, err := lustre.ParseFid(fmt.Sprintf("0xdead:0x%x:0x0", id))
	if err != nil {
		t.Fatalf("error generating test fid: %s", err)
	}
	return testFid
}

func TestArchiveEndToEnd(t *testing.T) {
	// NB: Leaktest finds a leak in the go-metrics library, but everything
	// else seems fine.
	if enableLeakTest {
		defer leaktest.Check(t)()
	}

	// First, start a test agent to delegate work to test data movers.
	as := hsm.NewTestSource()

	ta := testStartAgent(t, as)
	defer ta.Stop()

	// Now, start a data mover plugin which will connect to our
	// test agent to receive an injected action.
	tm := testStartMover(t)
	defer tm.Stop()

	for i := 0; i < testReps; i++ {
		testFid := testGenFid(t, i)

		// Inject an action
		tr := hsm.NewTestRequest(uint(testArchiveID), llapi.HsmActionArchive, testFid, nil)
		as.Inject(tr)

		// Wait for the mover to signal that it has received the action
		// on the other side of the RPC interface
		action := <-tm.ReceivedAction()
		actionPath := action.PrimaryPath()
		fidPath := fs.FidRelativePath(testFid)
		if actionPath != fidPath {
			debug.Printf("%d: received nil action", i)
			t.Fatalf("expected path %s, got %s", fidPath, actionPath)
		}

		// Wait for the mover to send a progress update on the action
		update := <-tr.ProgressUpdates()
		debug.Printf("Update: %v", update)

		// Wait for the mover to end the request
		<-tr.Finished()
	}
}

func TestRestoreEndToEnd(t *testing.T) {
	if enableLeakTest {
		defer leaktest.Check(t)()
	}

	// First, start a test agent to delegate work to test data movers.
	as := hsm.NewTestSource()
	ta := testStartAgent(t, as)
	defer ta.Stop()

	// Now, start a data mover plugin which will connect to our
	// test agent to receive an injected action.
	tm := testStartMover(t)
	defer tm.Stop()

	for i := 0; i < testReps; i++ {
		testFid := testGenFid(t, i)

		fileid.Set(fs.FidRelativePath(testFid), []byte("moo"))
		// Inject an action
		tr := hsm.NewTestRequest(uint(testArchiveID), llapi.HsmActionRestore, testFid, nil)
		as.Inject(tr)

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
}

func TestRemoveEndToEnd(t *testing.T) {
	if enableLeakTest {
		defer leaktest.Check(t)()
	}

	// First, start a test agent to delegate work to test data movers.
	as := hsm.NewTestSource()
	ta := testStartAgent(t, as)
	defer ta.Stop()

	// Now, start a data mover plugin which will connect to our
	// test agent to receive an injected action.
	tm := testStartMover(t)
	defer tm.Stop()

	for i := 0; i < testReps; i++ {
		testFid := testGenFid(t, i)
		fileid.Set(fs.FidRelativePath(testFid), []byte("moo"))
		// Inject an action
		tr := hsm.NewTestRequest(uint(testArchiveID), llapi.HsmActionRemove, testFid, nil)
		as.Inject(tr)

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
}
