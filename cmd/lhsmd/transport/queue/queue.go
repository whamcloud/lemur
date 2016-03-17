package queue

import (
	"flag"
	"sync"

	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/audit"
	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/lustre/hsm"
	"github.intel.com/hpdd/lustre/llapi"
	"github.intel.com/hpdd/policy/pdm"
	"github.intel.com/hpdd/policy/pdm/lhsmd/agent"
	"github.intel.com/hpdd/policy/pkg/workq"
)

// TransportType is the name of this transport
const TransportType = "queue"

type (
	// AgentEndpoint represents the agent side of a data mover connection
	AgentEndpoint struct {
		queue    *workq.Master
		mu       sync.Mutex
		requests map[uint64]hsm.ActionHandle
	}
	queueTransport struct{}
)

var reset, trace bool

func init() {
	flag.BoolVar(&reset, "reset", false, "Reset queue")
	flag.BoolVar(&trace, "trace", false, "Print redis trace")

	agent.RegisterTransport(&queueTransport{})
}

func (t *queueTransport) Init(conf *agent.Config, a *agent.HsmAgent) error {
	if _, ok := conf.Transports[TransportType]; !ok {
		return nil
	}

	connStr := conf.Transports[TransportType].ConnectionString()
	if reset {
		audit.Log("Reseting pdm queue")
		workq.MasterReset("pdm", connStr)
	}
	audit.Log("Initializing queue transport")
	qep := &AgentEndpoint{
		queue:    workq.NewMaster("pdm", connStr),
		requests: make(map[uint64]hsm.ActionHandle),
	}
	a.Endpoints.Add(1, qep)
	qep.queue.AddReceiver(qep)
	return nil
}

func hsm2pdmCommand(a llapi.HsmAction) (c pdm.CommandType) {
	switch a {
	case llapi.HsmActionArchive:
		c = pdm.ArchiveCommand
	case llapi.HsmActionRestore:
		c = pdm.RestoreCommand
	case llapi.HsmActionRemove:
		c = pdm.RemoveCommand
	case llapi.HsmActionCancel:
		c = pdm.CancelCommand
	default:
		alert.Fatalf("unknown command: %v", a)
	}

	return
}

// Send delivers an agent action to the backend
func (ep *AgentEndpoint) Send(action *agent.Action) {
	aih := action.Handle()
	req := &pdm.Request{
		Agent:  "me",
		Cookie: aih.Cookie(),
		//		SourcePath: fs.FidPath(ep.root, aih.Fid()),
		Endpoint: "posix",
		Command:  hsm2pdmCommand(aih.Action()),
		Archive:  aih.ArchiveID(),
		Offset:   aih.Offset(),
		Length:   aih.Length(),
		Params:   "",
	}

	ep.mu.Lock()
	ep.requests[aih.Cookie()] = aih
	ep.mu.Unlock()
	err := ep.queue.Send(req)
	if err != nil {
		alert.Fatal(err)
	}

}

// Update relays a data mover status update back to the HSM coordinator
func (ep *AgentEndpoint) Update(d workq.StatusDelivery) error {
	reply := &pdm.Result{}
	if err := d.Payload(reply); err != nil {
		audit.Log(err)
		return err
	}
	debug.Printf("reply: %v", reply)

	ep.mu.Lock()
	defer ep.mu.Unlock()
	if aih, ok := ep.requests[reply.Cookie]; ok {
		delete(ep.requests, reply.Cookie)
		debug.Printf("end: %s", aih)
		aih.End(0, 0, 0, -1)
	} else {
		debug.Printf("! unknown handle: %d", reply.Cookie)
	}
	return nil
}
