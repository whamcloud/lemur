package rpc

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.intel.com/hpdd/lemur/cmd/lhsmd/agent"
	pb "github.intel.com/hpdd/lemur/pdm"
	"github.intel.com/hpdd/logging/debug"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const (
	// TransportType is the name of this transport
	TransportType = "grpc"
	// Connected indicates a connected endpoint
	Connected = EndpointState(iota)
	// Disconnected indicates a disconnected endpoint
	Disconnected
)

type (
	rpcTransport struct {
		mu     sync.Mutex
		server *grpc.Server
	}

	dmRPCServer struct {
		stats *messageStats
		agent *agent.HsmAgent
	}

	// EndpointState represents the connectedness state of an Endpoint
	EndpointState int

	// AgentEndpoint represents the agent side of a data mover connection
	AgentEndpoint struct {
		state    EndpointState
		archive  int
		actionCh chan *agent.Action
		mu       sync.Mutex
		actions  map[agent.ActionID]*agent.Action
	}
)

func init() {
	agent.RegisterTransport(TransportType, &rpcTransport{})
}

func (t *rpcTransport) Init(conf *agent.Config, a *agent.HsmAgent) error {
	if conf.Transport.Type != TransportType {
		return nil
	}

	debug.Print("Initializing grpc transport")
	sock, err := net.Listen("tcp", conf.Transport.ConnectionString())
	if err != nil {
		return errors.Errorf("Failed to listen: %v", err)
	}

	t.mu.Lock()
	t.server = grpc.NewServer()
	t.mu.Unlock()
	pb.RegisterDataMoverServer(t.server, newServer(a))
	go t.server.Serve(sock)

	return nil
}

func (t *rpcTransport) Shutdown() {
	t.mu.Lock()
	t.server.Stop()
	t.mu.Unlock()
	debug.Print("shut down grpc transport")
}

// Send delivers an agent action to the backend
func (ep *AgentEndpoint) Send(action *agent.Action) {
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

func (s *dmRPCServer) Register(context context.Context, e *pb.Endpoint) (*pb.Handle, error) {
	ep, ok := s.agent.Endpoints.Get(e.Archive)
	var handle *agent.Handle
	var err error
	if ok {
		rpcEp, ok := ep.(*AgentEndpoint)
		if !ok {
			debug.Printf("not an rpc endpoint: %#v", ep)
			return nil, errors.Errorf("not an rpc endpoint: %#v", ep)
		}
		if rpcEp.state == Connected {
			debug.Printf("register rejected for  %v already connected", e)
			return nil, errors.New("Archived already connected")
		}
		// TODO: should flush and perhaps even delete the existing Endpoint
		// instead of just reusing it.
		handle, err = s.agent.Endpoints.NewHandle(e.Archive)
		if err != nil {
			return nil, err
		}
	} else {
		handle, err = s.agent.Endpoints.Add(e.Archive, &AgentEndpoint{
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

func (s *dmRPCServer) GetActions(h *pb.Handle, stream pb.DataMover_GetActionsServer) error {
	temp, ok := s.agent.Endpoints.GetWithHandle((*agent.Handle)(&h.Id))
	if !ok {
		debug.Printf("bad cookie  %v", h.Id)
		return errors.New("bad cookie")
	}
	ep, ok := temp.(*AgentEndpoint)
	if !ok {
		debug.Printf("not an rpc endpoint: %#v", ep)
		return errors.Errorf("not an rpc endpoint: %#v", ep)
	}

	/* Should use atomic CAS here */
	ep.state = Connected
	defer func() {
		debug.Printf("user disconnected %v", h)
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
				debug.Printf("error while sending action: %s", err)
				action.Fail(-1)

				ep.mu.Lock()
				delete(ep.actions, action.ID())
				ep.mu.Unlock()

				return errors.Wrap(err, "sending action failed")
			}
		}
	}
}

/*
* StatusStream provides the server with a stream of replies from the backend.
* The backend includes its cookie in each reply. In theory it's possible for
* replies to arrive for a Disconnected Endpoint, so we'll need proper protection
* from various kinds of races here.
 */

func (s *dmRPCServer) StatusStream(stream pb.DataMover_StatusStreamServer) error {
	for {
		status, err := stream.Recv()
		if err != nil {
			return nil
		}
		temp, ok := s.agent.Endpoints.GetWithHandle((*agent.Handle)(&status.Handle.Id))
		if !ok {
			debug.Printf("bad handle %v", status.Handle)
			return errors.New("bad endpoint handle")
		}
		ep, ok := temp.(*AgentEndpoint)
		if !ok {
			debug.Printf("not an rpc endpoint: %#v", ep)
			return errors.Errorf("not an rpc endpoint: %#v", ep)
		}

		ep.mu.Lock()
		action, ok := ep.actions[agent.ActionID(status.Id)]
		ep.mu.Unlock()
		if ok {
			completed, err := action.Update(status)
			if completed {
				ep.mu.Lock()
				delete(ep.actions, agent.ActionID(status.Id))
				ep.mu.Unlock()
			} else if err != nil {
				debug.Printf("Status update for 0x%x did not complete: %s", status.Id, err)
				ep.mu.Lock()
				delete(ep.actions, agent.ActionID(status.Id))
				ep.mu.Unlock()

				// send cancel to mover
			}
		} else {
			debug.Printf("! unknown id: %x", status.Id)
		}

	}
}

func (s *dmRPCServer) startStats() {
	go func() {
		for {
			fmt.Println(s.stats)
			time.Sleep(10 * time.Second)
		}
	}()
}

func newServer(a *agent.HsmAgent) *dmRPCServer {
	srv := &dmRPCServer{
		stats: newMessageStats(),
		agent: a,
	}

	//	srv.startStats()

	return srv
}
