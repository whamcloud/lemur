package rpc

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.intel.com/hpdd/liblog"
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

	dmRpcServer struct {
		stats *messageStats
		agent *agent.HsmAgent
	}

	EndpointState int

	RpcEndpoint struct {
		state    EndpointState
		archive  int
		actionCh chan *agent.Action
		mu       sync.Mutex
		actions  map[agent.ActionID]*agent.Action
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

func (ep *RpcEndpoint) Send(action *agent.Action) {
	ep.actionCh <- action
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

func (s *dmRpcServer) Register(context context.Context, e *pb.Endpoint) (*pb.Handle, error) {
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
			actions:  make(map[agent.ActionID]*agent.Action),
			actionCh: make(chan *agent.Action),
		})
		if err != nil {
			return nil, err
		}
	}
	return &pb.Handle{Id: uint64(*handle)}, nil

}

/*
 * GetActions establish a connection the backend for a particular archive ID. The Endpoint
* remains in Connected status as long as the backend is receiving messages from the agent.
*/

func (s *dmRpcServer) GetActions(h *pb.Handle, stream pb.DataMover_GetActionsServer) error {
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
		case action := <-ep.actionCh:
			s.stats.Count.Inc(1)
			s.stats.Rate.Mark(1)

			ep.mu.Lock()
			ep.actions[action.ID()] = action
			ep.mu.Unlock()

			if err := stream.Send(action.AsMessage()); err != nil {
				liblog.Debug(err)
				action.Fail(-1)

				ep.mu.Lock()
				delete(ep.actions, action.ID())
				ep.mu.Unlock()

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

func (s *dmRpcServer) StatusStream(stream pb.DataMover_StatusStreamServer) error {
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

		ep.mu.Lock()
		action, ok := ep.actions[agent.ActionID(status.Id)]
		ep.mu.Unlock()
		if ok {
			completed, err := action.Update(status)
			if completed && err == nil {
				ep.mu.Lock()
				delete(ep.actions, agent.ActionID(status.Id))
				ep.mu.Unlock()
			} else if err != nil {
				ep.mu.Lock()
				delete(ep.actions, agent.ActionID(status.Id))
				ep.mu.Unlock()

				// send cancel to mover
			}
		} else {
			liblog.Debug("! unknown id: %x", status.Id)
		}

	}
}

func (s *dmRpcServer) startStats() {
	go func() {
		for {
			fmt.Println(s.stats)
			time.Sleep(10 * time.Second)
		}
	}()
}

func newServer(a *agent.HsmAgent) *dmRpcServer {
	srv := &dmRpcServer{
		stats: newMessageStats(),
		agent: a,
	}

	//	srv.startStats()

	return srv
}
