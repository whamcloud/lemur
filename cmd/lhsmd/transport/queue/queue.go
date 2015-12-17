package queue

import (
	"log"
	"sync"

	"github.intel.com/hpdd/lustre/hsm"
	"github.intel.com/hpdd/lustre/llapi"
	"github.intel.com/hpdd/policy/pdm"
	"github.intel.com/hpdd/policy/pdm/lhsmd/agent"
	"github.intel.com/hpdd/policy/pkg/workq"
)

type (
	QueueEndpoint struct {
		queue    *workq.Master
		mu       sync.Mutex
		requests map[uint64]hsm.ActionHandle
	}
	queueTransport struct{}
)

func init() {
	agent.RegisterTransport(&queueTransport{})
}

func (t *queueTransport) Init(conf *pdm.HSMConfig, a *agent.HsmAgent) {
	log.Println("Initializing queue transport")
	qep := &QueueEndpoint{
		queue:    workq.NewMaster("pdm", conf.RedisServer),
		requests: make(map[uint64]hsm.ActionHandle),
	}
	a.Endpoints.Add(1, qep)
	qep.queue.AddReceiver(qep)
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
		log.Fatalf("unknown command: %v", a)
	}

	return
}

func (ep *QueueEndpoint) Send(aih hsm.ActionHandle) {
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
		log.Fatal(err)
	}

}

func (ep *QueueEndpoint) Update(d workq.StatusDelivery) error {
	reply := &pdm.Result{}
	if err := d.Payload(reply); err != nil {
		log.Println(err)
		return err
	}
	log.Printf("reply: %v\n", reply)

	ep.mu.Lock()
	defer ep.mu.Unlock()
	if aih, ok := ep.requests[reply.Cookie]; ok {
		delete(ep.requests, reply.Cookie)
		log.Printf("end: %s", aih)
		aih.End(0, 0, 0, -1)
	} else {
		log.Printf("! unknown handle: %s", reply.Cookie)
	}
	return nil
}
