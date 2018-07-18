// Copyright (c) 2016 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package main_test

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/pkg/errors"

	"golang.org/x/net/context"

	"github.com/intel-hpdd/go-lustre"
	"github.com/intel-hpdd/go-lustre/fs"
	"github.com/intel-hpdd/go-lustre/hsm"
	"github.com/intel-hpdd/go-lustre/llapi"
	"github.com/intel-hpdd/lemur/cmd/lhsmd/agent"
	"github.com/intel-hpdd/lemur/cmd/lhsmd/agent/fileid"
	"github.com/intel-hpdd/lemur/cmd/lhsmd/config"
	_ "github.com/intel-hpdd/lemur/cmd/lhsmd/transport/grpc"
	"github.com/intel-hpdd/lemur/dmplugin"
	"github.com/intel-hpdd/lemur/pkg/fsroot"
	"github.com/intel-hpdd/logging/debug"
)

const (
	testSocketDir = "/tmp"
	testArchiveID = 1
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

	testMoverData struct {
		UUID        string
		Length      int64
		Errval      int
		UpdateCount int
	}
)

// Archive tests archive requests
// * The request data is used as the fileID
func (t *testMover) Archive(a dmplugin.Action) error {
	debug.Printf("testMover received Archive action: %s", a)
	t.receivedAction <- a
	var data testMoverData
	err := json.Unmarshal(a.Data(), &data)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("parsing '%s'", string(a.Data())))
	}

	if data.UUID != "" {
		a.SetUUID(data.UUID)
	}
	if data.Length > 0 {
		a.SetActualLength(data.Length)
	}

	if data.UpdateCount > 0 {
		var offset int64
		length := data.Length / int64(data.UpdateCount)
		for i := 0; i < data.UpdateCount; i++ {
			a.Update(offset, length, data.Length)
			offset += length
		}
	}
	if data.Errval != 0 {
		return errors.New("We failed")
	}
	return nil
}

func (t *testMover) Restore(a dmplugin.Action) error {
	debug.Printf("testMover received Restore action: %s", a)
	t.receivedAction <- a
	var data testMoverData
	err := json.Unmarshal(a.Data(), &data)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("parsing '%s'", string(a.Data())))
	}

	if data.Length > 0 {
		a.SetActualLength(data.Length)
	}

	if data.UpdateCount > 0 {
		var offset int64
		length := data.Length / int64(data.UpdateCount)
		for i := 0; i < data.UpdateCount; i++ {
			a.Update(offset, length, data.Length)
			offset += length
		}
	}

	if data.Errval != 0 {
		return errors.New("We failed")
	}

	return nil
}

func (t *testMover) Remove(a dmplugin.Action) error {
	debug.Printf("testMover received Remove action: %s", a)
	t.receivedAction <- a

	var data testMoverData
	err := json.Unmarshal(a.Data(), &data)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("parsing '%s'", string(a.Data())))
	}

	if data.Length > 0 {
		a.SetActualLength(data.Length)
	}

	if data.UpdateCount > 0 {
		var offset int64
		length := data.Length / int64(data.UpdateCount)
		for i := 0; i < data.UpdateCount; i++ {
			a.Update(offset, length, data.Length)
			offset += length
		}
	}

	if data.Errval != 0 {
		return errors.New("We failed")
	}

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

	cases := []testMoverData{
		{Length: 100, UpdateCount: 1},
		{Length: 10000, UpdateCount: 5},
		{UpdateCount: 1, Errval: -1},
		{Errval: -1},
	}

	for i, expected := range cases {
		testFid := testGenFid(t, i)
		if expected.UUID == "" {
			expected.UUID = fmt.Sprintf("testid-%x", i)
		}
		adata, err := agent.MarshalActionData(nil, &expected)
		if err != nil {
			t.Fatal(err)
		}

		// Inject an action
		tr := hsm.NewTestRequest(uint(testArchiveID), llapi.HsmActionArchive, testFid, adata)
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
		updateCount := 0
		for update := range tr.ProgressUpdates() {
			updateCount++
			debug.Printf("Update: %v", update)
			if update.Cookie != tr.Cookie() {
				t.Fatalf("cookie mismatch request: %v  update: %v", tr.Cookie(), update.Cookie)
			}
			if update.Complete {
				if expected.Errval != 0 {
					if update.Errval != expected.Errval {
						t.Fatalf("Errval expected %v != %v", expected.Errval, update.Errval)
					}
					continue
				}

				buf, _ := fileid.UUID.GetByFid(fs.RootDir{}, testFid)
				if string(buf) != expected.UUID {
					t.Fatalf("fileID invalid '%s'", buf)
				}
				if update.Length != expected.Length {
					t.Fatalf("Length expected %v != %v", expected.Length, update.Length)
				}
			}
		}
		if updateCount-1 != expected.UpdateCount {
			t.Fatalf("UpdateCount expected %v != %v", expected.UpdateCount, updateCount-1)
		}
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

	cases := []testMoverData{
		{Length: 100, UpdateCount: 1},
		{Length: 10000, UpdateCount: 5},
		{UpdateCount: 1, Errval: -1},
		{Errval: -1},
	}

	for i, expected := range cases {
		testFid := testGenFid(t, i)

		fileid.UUID.Set(fs.FidRelativePath(testFid), []byte("moo"))
		// Inject an action
		adata, err := agent.MarshalActionData(nil, &expected)
		if err != nil {
			t.Fatal(err)
		}

		tr := hsm.NewTestRequest(uint(testArchiveID), llapi.HsmActionRestore, testFid, adata)
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
		updateCount := 0
		for update := range tr.ProgressUpdates() {
			updateCount++
			debug.Printf("Update: %v", update)
			if update.Cookie != tr.Cookie() {
				t.Fatalf("cookie mismatch request: %v  update: %v", tr.Cookie(), update.Cookie)
			}
			if update.Complete {
				if expected.Errval != 0 {
					if update.Errval != expected.Errval {
						t.Fatalf("Errval expected %v != %v", expected.Errval, update.Errval)
					}
					continue
				}

				if update.Length != expected.Length {
					t.Fatalf("Length expected %v != %v", expected.Length, update.Length)
				}
			}
		}
		if updateCount-1 != expected.UpdateCount {
			t.Fatalf("UpdateCount expected %v != %v", expected.UpdateCount, updateCount-1)
		}
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

	cases := []testMoverData{
		{Length: 100},
		{Length: 10000},
		{Errval: -1},
		{Errval: -1},
	}

	for i, expected := range cases {
		testFid := testGenFid(t, i)
		fileid.UUID.Set(fs.FidRelativePath(testFid), []byte("moo"))
		// Inject an action
		adata, err := agent.MarshalActionData(nil, &expected)
		if err != nil {
			t.Fatal(err)
		}

		tr := hsm.NewTestRequest(uint(testArchiveID), llapi.HsmActionRemove, testFid, adata)
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
		updateCount := 0
		for update := range tr.ProgressUpdates() {
			updateCount++
			debug.Printf("Update: %v", update)
			if update.Cookie != tr.Cookie() {
				t.Fatalf("cookie mismatch request: %v  update: %v", tr.Cookie(), update.Cookie)
			}
			if update.Complete {
				if expected.Errval != 0 {
					if update.Errval != expected.Errval {
						t.Fatalf("Errval expected %v != %v", expected.Errval, update.Errval)
					}
					continue
				}

				if update.Length != expected.Length {
					t.Fatalf("Length expected %v != %v", expected.Length, update.Length)
				}
			}
		}

		// The -1 is because End message always happens.
		if updateCount-1 != expected.UpdateCount {
			t.Fatalf("UpdateCount expected %v != %v", expected.UpdateCount, updateCount-1)
		}
	}
}
