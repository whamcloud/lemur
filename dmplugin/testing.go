package dmplugin

import (
	"math"
	"path"
	"testing"

	"github.intel.com/hpdd/logging/alert"
)

type TestAction struct {
	t            *testing.T
	id           uint64
	path         string
	offset       uint64
	length       uint64
	data         []byte
	fileID       []byte
	ActualLength int
}

// NewTestAction returns a stub action that can be used for testing.
func NewTestAction(t *testing.T, path string, offset uint64, length uint64, fileID []byte, data []byte) *TestAction {
	return &TestAction{
		t:      t,
		id:     1,
		path:   path,
		offset: offset,
		length: length,
		fileID: fileID,
		data:   data,
	}
}

// Update sends an action status update
func (a *TestAction) Update(offset, length, max uint64) error {
	return nil
}

// Complete signals that the action has completed
func (a *TestAction) Complete() error {
	return nil
}

// Fail signals that the action has failed
func (a *TestAction) Fail(err error) error {
	alert.Warnf("fail: id:%d %v", a.id, err)
	return nil
}

// ID returns the action item's ID
func (a *TestAction) ID() uint64 {
	return a.id
}

// Offset returns the current offset of the action item
func (a *TestAction) Offset() uint64 {
	return a.offset
}

// Length returns the expected length of the action item's file
func (a *TestAction) Length() uint64 {
	return a.length
}

// Data returns a byte slice of the action item's data
func (a *TestAction) Data() []byte {
	return a.data
}

// PrimaryPath returns the action item's primary file path
func (a *TestAction) PrimaryPath() string {
	return a.path
}

// WritePath returns the action item's write path (e.g. for restores)
func (a *TestAction) WritePath() string {
	return a.path
}

// FileID returns the action item's file id
func (a *TestAction) FileID() []byte {
	return a.fileID
}

// SetFileID sets the action's file id
func (a *TestAction) SetFileID(id []byte) {
	a.fileID = id
}

// SetActualLength sets the action's actual file length
func (a *TestAction) SetActualLength(length uint64) {
	if a.length != math.MaxUint64 && length != a.length {
		a.t.Fatalf("actual length does not match original %d !=%d", length, a.length)
	}
	a.ActualLength = int(length)
}

type testPlugin struct {
	name   string
	config *pluginConfig
	t      *testing.T
}

// NewTestPlugin returns a test plugin
func NewTestPlugin(t *testing.T, name string) Plugin {
	return &testPlugin{
		config: mustInitConfig(),
		name:   name,
		t:      t,
	}
}

// Base returns the root directory for plugin.
func (a *testPlugin) Base() string {
	return a.config.ClientRoot
}

// ConfigFile returns path to the plugin config file.
func (a *testPlugin) ConfigFile() string {
	return path.Join(a.config.ConfigDir, a.name)
}

// AddMover registers a new data mover with the plugin
func (a *testPlugin) AddMover(config *Config) {
	a.t.Fatal("AddMover not implemeneted in testPlugin")
}

// Stop signals to all registered data movers that they should stop processing
// and shut down
func (a *testPlugin) Stop() {
}

// Close closes the connection to the agent
func (a *testPlugin) Close() error {
	return nil
}
