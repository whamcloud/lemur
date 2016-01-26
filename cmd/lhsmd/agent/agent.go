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
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.intel.com/hpdd/liblog"
	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/hsm"
	"github.intel.com/hpdd/policy/pdm"
	"github.intel.com/hpdd/policy/pkg/client"
)

type (
	// HsmAgent for a single filesytem and a collection of backends.
	HsmAgent struct {
		client    *client.Client
		agent     hsm.Agent
		wg        sync.WaitGroup
		Endpoints *Endpoints
	}

	// Transport for backend plugins
	Transport interface {
		Init(*pdm.HSMConfig, *HsmAgent)
	}
)

func (ct *HsmAgent) Stop() {
	if ct.agent != nil {
		ct.agent.Stop()
	}
}

func (ct *HsmAgent) Root() fs.RootDir {
	return ct.client.Root()
}

func (ct *HsmAgent) initAgent(done chan struct{}) error {
	var err error
	ct.agent, err = hsm.Start(ct.client.Root(), done)

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

func RegisterTransport(t Transport) {
	transports = append(transports, t)
}

func Daemon(conf *pdm.HSMConfig) error {
	client, err := client.New(conf.Lustre)
	if err != nil {
		return err
	}

	done := make(chan struct{})
	ct := &HsmAgent{
		client:    client,
		Endpoints: NewEndpoints(),
	}

	interruptHandler(func() {
		close(done)
		ct.Stop()
	})

	for _, t := range transports {
		t.Init(conf, ct)
	}

	errChan := make(chan error)
	go func() {
		err := ct.initAgent(done)
		if err != nil {
			errChan <- err
			return
		}

		for i := 0; i < conf.Processes; i++ {
			ct.addHandler()
		}
	}()

	for {
		select {
		case <-done:
			ct.wg.Wait()
			return nil
		case err := <-errChan:
			return err
		}
	}
}

func interruptHandler(once func()) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGQUIT, syscall.SIGTERM)

	go func() {
		stopping := false
		for sig := range c {
			liblog.Debug("signal received: %s", sig)
			if !stopping {
				stopping = true
				once()
			}
		}
	}()

}
