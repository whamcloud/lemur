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
	"sync"

	"golang.org/x/net/context"

	"github.intel.com/hpdd/liblog"
	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/hsm"
	"github.intel.com/hpdd/policy/pdm"
	"github.intel.com/hpdd/policy/pkg/client"
)

type (
	// HsmAgent for a single filesytem and a collection of backends.
	HsmAgent struct {
		context context.Context
		cancel  context.CancelFunc

		config    *pdm.HSMConfig
		client    *client.Client
		agent     hsm.Agent
		wg        sync.WaitGroup
		Endpoints *Endpoints
	}

	// Transport for backend plugins
	Transport interface {
		Init(*pdm.HSMConfig, *HsmAgent) error
	}
)

// New accepts a config and returns a *HsmAgent
func New(cfg *pdm.HSMConfig) (*HsmAgent, error) {
	client, err := client.New(cfg.Lustre)
	if err != nil {
		return nil, err
	}

	ct := &HsmAgent{
		config:    cfg,
		client:    client,
		Endpoints: NewEndpoints(),
	}

	return ct, nil
}

// Start backgrounds the agent and starts backend data movers
func (ct *HsmAgent) Start(ctx context.Context) error {
	ct.context, ct.cancel = context.WithCancel(ctx)

	for _, t := range transports {
		if err := t.Init(ct.config, ct); err != nil {
			return err
		}
	}

	if err := ct.initAgent(ct.context); err != nil {
		return err
	}

	for i := 0; i < ct.config.Processes; i++ {
		ct.addHandler()
	}

	for {
		select {
		case <-ct.context.Done():
			ct.wg.Wait()
			return nil
		}
	}
}

// Stop shuts down all backend data movers and kills the agent
func (ct *HsmAgent) Stop() {
	if ct.agent != nil {
		ct.agent.Stop()
	}

	if ct.cancel != nil {
		ct.cancel()
	}
}

// Root returns a fs.RootDir representing the Lustre filesystem root
func (ct *HsmAgent) Root() fs.RootDir {
	return ct.client.Root()
}

func (ct *HsmAgent) initAgent(ctx context.Context) error {
	var err error
	ct.agent, err = hsm.Start(ct.client.Root(), ctx)

	if err != nil {
		return err
	}

	return nil
}

func (ct *HsmAgent) handleActions() {

	ch := ct.agent.Actions()
	for ai := range ch {
		liblog.Debug("incoming: %s", ai)
		aih, err := ai.Begin(0, false)
		if err != nil {
			liblog.Debug("begin failed: %v", err)
			continue
		}

		if e, ok := ct.Endpoints.Get(uint32(aih.ArchiveID())); ok {
			liblog.Debug("Request: %v", aih)
			e.Send(aih)
		} else {
			liblog.Debug("No handler for archive %d", aih.ArchiveID())
			aih.End(0, 0, 0, -1)
		}

	}
}

func (ct *HsmAgent) addHandler() {
	ct.wg.Add(1)
	go func() {
		defer ct.wg.Done()
		ct.handleActions()
	}()
}

var transports []Transport

// RegisterTransport registers the transport in the list of known transports
func RegisterTransport(t Transport) {
	transports = append(transports, t)
}
