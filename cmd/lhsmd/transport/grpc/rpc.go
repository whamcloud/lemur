package rpc

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.intel.com/hpdd/liblog"
	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/hsm"
	"github.intel.com/hpdd/lustre/llapi"
	"github.intel.com/hpdd/policy/pdm/lhsmd/agent"
	pb "github.intel.com/hpdd/policy/pdm/pdm"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const (
	Connected = EndpointState(iota)
	Disconnected
)

type (
	rpcTransport struct{}

	dataMoverServer struct {
		stats *messageStats
		agent *agent.HsmAgent
	}

	EndpointState int

	RpcEndpoint struct {
		state    EndpointState
		archive  int
		actionCh chan hsm.ActionHandle
		mu       sync.Mutex
		actions  map[uint64]hsm.ActionHandle
	}
)

func init() {
	agent.RegisterTransport(&rpcTransport{})
}

func (t *rpcTransport) Init(conf *agent.Config, a *agent.HsmAgent) error {
	liblog.Debug("Initializing grpc transport")
	addr := fmt.Sprintf(":%d", conf.RPCPort)
	sock, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("Failed to listen: %v", err)
	}

	srv := grpc.NewServer()
	pb.RegisterDataMoverServer(srv, newServer(a))
	go srv.Serve(sock)

	return nil
}

func (ep *RpcEndpoint) Send(aih hsm.ActionHandle) {

	ep.actionCh <- aih

}

/*
 * Register a data mover backend (aka Endpoint). When a backend starts, it first must
 * identify itself and its archive ID with the agent. The agent returns a unique
 * cookie that the backend uses for the rest of that session.
 *
 * If the Endpoint for this archive id already exists and is Connected, then this means
 * this is already a backend receiving messages for this archive, and we reject
 * this registration.  If it exists and is Disconnected, then currently the new backend
 * takes over this Endpoint. Existing in progress messages should be flushed, however.
 */

func (s *dataMoverServer) Register(context context.Context, e *pb.Endpoint) (*pb.Handle, error) {
	ep, ok := s.agent.Endpoints.Get(e.Archive)
	var handle *agent.Handle
	var err error
	if ok {
		rpcEp, ok := ep.(*RpcEndpoint)
		if !ok {
			log.Fatalf("not an rpc endpoint: %#v", ep)
		}
		if rpcEp.state == Connected {
			liblog.Debug("register rejected for  %v already connected", e)
			return nil, errors.New("Archived already connected")
		} else {
			// TODO: should flush and perhaps even delete the existing Endpoint
			// instead of just reusing it.
			handle, err = s.agent.Endpoints.NewHandle(e.Archive)
			if err != nil {
				return nil, err
			}

		}
	} else {
		handle, err = s.agent.Endpoints.Add(e.Archive, &RpcEndpoint{
			state:    Disconnected,
			actions:  make(map[uint64]hsm.ActionHandle),
			actionCh: make(chan hsm.ActionHandle),
		})
		if err != nil {
			return nil, err
		}
	}
	return &pb.Handle{Id: uint64(*handle)}, nil

}

func hsm2Command(a llapi.HsmAction) (c pb.Command) {
	switch a {
	case llapi.HsmActionArchive:
		c = pb.Command_ARCHIVE
	case llapi.HsmActionRestore:
		c = pb.Command_RESTORE
	case llapi.HsmActionRemove:
		c = pb.Command_REMOVE
	case llapi.HsmActionCancel:
		c = pb.Command_CANCEL
	default:
		log.Fatalf("unknown command: %v", a)
	}

	return
}

/*
 * GetActions establish a connection the backend for a particular archive ID. The Endpoint
* remains in Connected status as long as the backend is receiving messages from the agent.
*/

func (s *dataMoverServer) GetActions(h *pb.Handle, stream pb.DataMover_GetActionsServer) error {
	temp, ok := s.agent.Endpoints.GetWithHandle((*agent.Handle)(&h.Id))
	if !ok {
		liblog.Debug("bad cookie  %v", h.Id)
		return errors.New("bad cookie")
	}
	ep, ok := temp.(*RpcEndpoint)
	if !ok {
		log.Fatalf("not an rpc endpoint: %#v", ep)
	}

	/* Should use atomic CAS here */
	ep.state = Connected
	defer func() {
		liblog.Debug("user disconnected %v", h)
		ep.state = Disconnected
		s.agent.Endpoints.RemoveHandle((*agent.Handle)(&h.Id))
	}()

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case aih := <-ep.actionCh:
			// liblog.Debug("Got %q from user, sending %d to stream", msg, id)
			s.stats.Count.Inc(1)
			s.stats.Rate.Mark(1)

			ep.mu.Lock()
			ep.actions[uint64(aih.Cookie())] = aih
			ep.mu.Unlock()

			item := &pb.ActionItem{
				Cookie:      aih.Cookie(),
				Op:          hsm2Command(aih.Action()),
				PrimaryPath: fs.FidRelativePath(aih.Fid()),
				Offset:      aih.Offset(),
				Length:      aih.Length(),
				Data:        aih.Data(),
			}

			switch aih.Action() {
			case llapi.HsmActionRestore, llapi.HsmActionRemove:
				var err error
				item.FileId, err = getFileID(s.agent.Root(), aih.Fid())
				if err != nil {
					log.Println(err) //hmm, can't restore if there is no file id
				}
			}

			dfid, err := aih.DataFid()
			if err == nil {
				item.WritePath = fs.FidRelativePath(dfid)
			}

			if err := stream.Send(item); err != nil {
				//			liblog.Debug("message %d failed to sen in %v", id, time.Since(ep.actions[id]))
				log.Println(err)
				aih.End(0, 0, 0, int(-1))
				return err
			}
		}
	}
	return nil
}

/*
* StatusStream provides the server with a stream of replies from the backend.
* The backend includes its cookie in each reply. In theory it's possible for
* replies to arrive for a Disconnected Endpoint, so we'll need proper protection
* from various kinds of races here.
 */

func (s *dataMoverServer) StatusStream(stream pb.DataMover_StatusStreamServer) error {
	for {
		status, err := stream.Recv()
		if err != nil {
			log.Println(err)
			return nil
		}
		temp, ok := s.agent.Endpoints.GetWithHandle((*agent.Handle)(&status.Handle.Id))
		if !ok {
			liblog.Debug("bad handle %v", status.Handle)
			return errors.New("bad endpoint handle")
		}
		ep, ok := temp.(*RpcEndpoint)
		if !ok {
			log.Fatalf("not an rpc endpoint: %#v", ep)
		}

		aih, ok := ep.actions[status.Cookie]
		if ok {
			liblog.Debug("Client acked message %x offset: %d length: %d complete: %v status: %d", status.Cookie,
				status.Offset,
				status.Length,
				status.Completed, status.Error)
			if status.Completed {
				if status.FileId != nil {
					updateFileID(s.agent.Root(), aih.Fid(), status.FileId)
				}
				aih.End(status.Offset, status.Length, 0, int(status.Error))
				ep.mu.Lock()
				delete(ep.actions, status.Cookie)
				ep.mu.Unlock()
			} else {
				aih.Progress(status.Offset, status.Length, aih.Length(), 0)
			}
			//		duration := time.Since(ep.actions[status.Cookie])
			//liblog.Debug("Client acked message %d status: %s in %v",
			//	ack.Id, nack.Status, duration)
			//		s.stats.Latencies.Update(duration.Nanoseconds())
		} else {
			liblog.Debug("! unknown cookie: %x", status.Cookie)
		}

	}
}

func (s *dataMoverServer) startStats() {
	go func() {
		for {
			fmt.Println(s.stats)
			time.Sleep(10 * time.Second)
		}
	}()
}

func newServer(a *agent.HsmAgent) *dataMoverServer {
	srv := &dataMoverServer{
		stats: newMessageStats(),
		agent: a,
	}

	//	srv.startStats()

	return srv
}
