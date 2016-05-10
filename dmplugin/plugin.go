package dmplugin

import (
	"path"

	pb "github.intel.com/hpdd/policy/pdm/pdm"
	"github.intel.com/hpdd/policy/pkg/client"
	"google.golang.org/grpc"
)

type dmPlugin struct {
	name     string
	rpcConn  *grpc.ClientConn
	cli      pb.DataMoverClient
	movers   []*DataMoverClient
	fsClient client.Client
	config   *pluginConfig
}

// Plugin represents a data mover plugin
type Plugin interface {
	AddMover(*Config)
	Stop()
	Close() error
	Base() string
	ConfigFile() string
}

// New returns a new *Plugin, or error
func New(name string) (Plugin, error) {
	config := mustInitConfig()

	fsClient, err := client.New(config.ClientRoot)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.Dial(config.AgentAddress, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return &dmPlugin{
		name:     name,
		rpcConn:  conn,
		cli:      pb.NewDataMoverClient(conn),
		fsClient: fsClient,
		config:   config,
	}, nil
}

// Base returns the root directory for plugin.
func (a *dmPlugin) Base() string {
	return a.fsClient.Path()
}

// ConfigFile returns path to the plugin config file.
func (a *dmPlugin) ConfigFile() string {
	return path.Join(a.config.ConfigDir, a.name)
}

// AddMover registers a new data mover with the plugin
func (a *dmPlugin) AddMover(config *Config) {
	dm := NewMover(a, a.cli, config)
	go dm.Run()
	a.movers = append(a.movers, dm)
}

// Stop signals to all registered data movers that they should stop processing
// and shut down
func (a *dmPlugin) Stop() {
	for _, dm := range a.movers {
		dm.Stop()
	}
}

// Close closes the connection to the agent
func (a *dmPlugin) Close() error {
	return a.rpcConn.Close()
}
