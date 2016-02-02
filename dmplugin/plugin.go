package dmplugin

import (
	pb "github.intel.com/hpdd/policy/pdm/pdm"
	"google.golang.org/grpc"
)

type Plugin struct {
	rpcConn *grpc.ClientConn
	cli     pb.DataMoverClient
	movers  []*DataMoverClient
}

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

func (a *Plugin) AddMover(mover Mover) {
	dm := NewMover(a.cli, mover)
	go dm.Run()
	a.movers = append(a.movers, dm)
}

func (a *Plugin) Stop() {
	for _, dm := range a.movers {
		dm.Stop()
	}
}

func (a *Plugin) Close() error {
	return a.rpcConn.Close()
}
