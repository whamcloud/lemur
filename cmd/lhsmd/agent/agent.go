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

	"github.com/pkg/errors"

	"golang.org/x/net/context"
	"golang.org/x/sys/unix"

	"github.intel.com/hpdd/lemur/pkg/fsroot"
	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/hsm"
	"github.intel.com/hpdd/lustre/llapi"
)

type (
	// HsmAgent for a single filesytem and a collection of backends.
	HsmAgent struct {
		config        *Config
		client        fsroot.Client
		stats         *ActionStats
		wg            sync.WaitGroup
		Endpoints     *Endpoints
		mu            sync.Mutex // Protect the agent
		actionSource  hsm.ActionSource
		monitor       *PluginMonitor
		cancelFunc    context.CancelFunc
		rpcsInFlight  chan struct{} // Buffered channel to throttle rpcs in flight
		startComplete chan struct{} // Closed when agent startup is completed
		stopComplete  chan struct{} // Closed when agent shutdown is completed
	}

	// Transport for backend plugins
	Transport interface {
		Init(*Config, *HsmAgent) error
		Shutdown()
	}
)

// New accepts a config and returns a *HsmAgent
func New(cfg *Config, client fsroot.Client, as hsm.ActionSource) (*HsmAgent, error) {
	ct := &HsmAgent{
		config:        cfg,
		client:        client,
		rpcsInFlight:  make(chan struct{}, cfg.Processes*10),
		stats:         NewActionStats(),
		monitor:       NewMonitor(),
		actionSource:  as,
		Endpoints:     NewEndpoints(),
		startComplete: make(chan struct{}),
		stopComplete:  make(chan struct{}),
	}

	return ct, nil
}

// Start backgrounds the agent and starts backend data movers
func (ct *HsmAgent) Start(ctx context.Context) error {
	ct.mu.Lock()
	ctx, ct.cancelFunc = context.WithCancel(ctx)
	ct.mu.Unlock()
	ct.stats.Start(ctx)

	if t, ok := transports[ct.config.Transport.Type]; ok {
		if err := t.Init(ct.config, ct); err != nil {
			return errors.Wrapf(err, "transport %q initialize failed", ct.config.Transport.Type)
		}
	} else {
		return errors.Errorf("unknown transport type in configuration: %s", ct.config.Transport.Type)
	}

	if err := ct.actionSource.Start(ctx); err != nil {
		return errors.Wrap(err, "initializing HSM agent connection")
	}

	for i := 0; i < ct.config.Processes; i++ {
		ct.addHandler(fmt.Sprintf("handler-%d", i))
	}

	ct.monitor.Start(ctx)
	for _, pluginConf := range ct.config.Plugins() {
		err := ct.monitor.StartPlugin(pluginConf)
		if err != nil {
			return errors.Wrapf(err, "creating plugin %q", pluginConf.Name)
		}
	}
	close(ct.startComplete)
	ct.wg.Wait()
	close(ct.stopComplete)
	return nil
}

// Stop shuts down all backend data movers and kills the agent
func (ct *HsmAgent) Stop() {
	ct.mu.Lock()
	ct.cancelFunc()
	ct.mu.Unlock()
	transports[ct.config.Transport.Type].Shutdown()
	<-ct.stopComplete
}

// StartWaitFor will wait for Agent to startup with time out of n.
func (ct *HsmAgent) StartWaitFor(n time.Duration) error {
	select {
	case <-ct.startComplete:
		return nil
	case <-time.After(n):
		return errors.Errorf("Agent startup timed out after %v", n)
	}

}

// Root returns a fs.RootDir representing the Lustre filesystem root
func (ct *HsmAgent) Root() fs.RootDir {
	return ct.client.Root()
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
			ai.FailImmediately(int(unix.EIO))
			continue
		}
		action := ct.newAction(aih)
		ct.rpcsInFlight <- struct{}{}
		ct.stats.StartAction(action)
		action.Prepare()
		if e, ok := ct.Endpoints.Get(uint32(aih.ArchiveID())); ok {
			debug.Printf("%s: id:%d new %s %x %v", tag, action.id,
				action.aih.Action(),
				action.aih.Cookie(),
				action.aih.Fid())
			e.Send(action)
		} else {
			alert.Warnf("no handler for archive %d", aih.ArchiveID())
			action.Fail(-1)
			ct.stats.CompleteAction(action, -1)
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
