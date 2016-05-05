/**

Parallel Data Mover is scalable system to copy or migrate data between
various storage systems. It supports multliple types of sources and
destinations, including POSIX, S3, HPSS, etc.

Use cases include:
  * Data movement for Lustre HSM.
  * Offsite replication for DR
  * Lustre file-level replication
  * Storage rebalancing within a single tier
  * Migration between filesytems (e.g GPFS - > Lustre)

Initially the main focus is for HSM.
*/

package agent

import (
	"fmt"
	"sync"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/sys/unix"

	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/hsm"
	"github.intel.com/hpdd/lustre/llapi"
	"github.intel.com/hpdd/policy/pkg/client"
)

type (
	// HsmAgent for a single filesytem and a collection of backends.
	HsmAgent struct {
		config       *Config
		client       client.Client
		wg           sync.WaitGroup
		Endpoints    *Endpoints
		mu           sync.Mutex // Protect the agent
		actionSource hsm.ActionSource
		monitor      *PluginMonitor
	}

	// Transport for backend plugins
	Transport interface {
		Init(*Config, *HsmAgent) error
		Shutdown() error
	}
)

// New accepts a config and returns a *HsmAgent
func New(cfg *Config, cl client.Client) (*HsmAgent, error) {

	ct := &HsmAgent{
		config:    cfg,
		client:    cl,
		monitor:   NewMonitor(),
		Endpoints: NewEndpoints(),
	}

	return ct, nil
}

// Start backgrounds the agent and starts backend data movers
func (ct *HsmAgent) Start(ctx context.Context) error {
	if t, ok := transports[ct.config.Transport.Type]; ok {
		if err := t.Init(ct.config, ct); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("Unknown transport type in configuration: %s", ct.config.Transport.Type)
	}

	if err := ct.initAgent(); err != nil {
		return fmt.Errorf("Unable to initialize HSM agent connection: %s", err)
	}

	for i := 0; i < ct.config.Processes; i++ {
		ct.addHandler(fmt.Sprintf("handler-%d", i))
	}

	ct.monitor.Start(ctx)
	for _, pluginConf := range ct.config.Plugins() {
		err := ct.monitor.StartPlugin(pluginConf)
		if err != nil {
			return fmt.Errorf("Error while creating plugin: %s", err)
		}
	}

	ct.wg.Wait()
	return nil
}

// Stop shuts down all backend data movers and kills the agent
func (ct *HsmAgent) Stop() {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	if err := transports[ct.config.Transport.Type].Shutdown(); err != nil {
		alert.Warnf("Error while shutting down transport: %s", err)
	}
	if ct.actionSource != nil {
		ct.actionSource.Stop()
	}
	if actionStats != nil {
		actionStats.Stop()
	}
}

// Root returns a fs.RootDir representing the Lustre filesystem root
func (ct *HsmAgent) Root() fs.RootDir {
	return ct.client.Root()
}

func (ct *HsmAgent) initAgent() (err error) {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	ct.actionSource, err = hsm.Start(ct.client.Root())
	return
}

func (ct *HsmAgent) newAction(aih hsm.ActionHandle) *Action {
	return &Action{
		id:    NextActionID(),
		aih:   aih,
		start: time.Now(),
		agent: ct,
	}
}

func (ct *HsmAgent) handleActions(tag string) {
	for ai := range ct.actionSource.Actions() {
		debug.Printf("%s: incoming: %s", tag, ai)
		// AFAICT, this is how the copytool is expected to handle cancels.
		if ai.Action() == llapi.HsmActionCancel {
			ai.FailImmediately(int(unix.ENOSYS))
			// TODO: send out of band cancel message to the mover
			continue
		}
		aih, err := ai.Begin(0, false)
		if err != nil {
			alert.Warnf("%s: begin failed: %v: %s", tag, err, ai)
			continue
		}
		action := ct.newAction(aih)
		StartAction(action)
		if e, ok := ct.Endpoints.Get(uint32(aih.ArchiveID())); ok {
			debug.Printf("%s: id:%d new %s %x %v", tag, action.id,
				action.aih.Action(),
				action.aih.Cookie(),
				action.aih.Fid())
			e.Send(action)
		} else {
			alert.Warnf("no handler for archive %d", aih.ArchiveID())
			action.Fail(-1)
			CompleteAction(action, -1)
		}
	}
}

func (ct *HsmAgent) addHandler(tag string) {
	ct.wg.Add(1)
	go func() {
		ct.handleActions(tag)
		ct.wg.Done()
	}()
}

var transports = map[string]Transport{}

// RegisterTransport registers the transport in the list of known transports
func RegisterTransport(name string, t Transport) {
	transports[name] = t
}
