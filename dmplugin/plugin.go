package dmplugin

import (
	pb "github.intel.com/hpdd/policy/pdm/pdm"
	"google.golang.org/grpc"
)

// Plugin represents a data mover plugin
type Plugin struct {
	rpcConn *grpc.ClientConn
	cli     pb.DataMoverClient
	movers  []*DataMoverClient
}

// New returns a new *Plugin, or error
func New(target string) (*Plugin, error) {
	conn, err := grpc.Dial(target, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	//	defer conn.Close()
	return &Plugin{
		rpcConn: conn,
		cli:     pb.NewDataMoverClient(conn),
	}, nil
}

// AddMover registers a new data mover with the plugin
func (a *Plugin) AddMover(mover Mover) {
	dm := NewMover(a.cli, mover)
	go dm.Run()
	a.movers = append(a.movers, dm)
}

// Stop signals to all registered data movers that they should stop processing
// and shut down
func (a *Plugin) Stop() {
	for _, dm := range a.movers {
		dm.Stop()
	}
}

// Close closes the connection to the agent
func (a *Plugin) Close() error {
	return a.rpcConn.Close()
}
