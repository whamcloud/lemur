package dmplugin

import (
	"net"
	"path"
	"sync"
	"time"

	"github.com/pkg/errors"

	"golang.org/x/net/context"

	pb "github.intel.com/hpdd/lemur/pdm"
	"github.intel.com/hpdd/lemur/pkg/fsroot"
	"google.golang.org/grpc"
)

type dmPlugin struct {
	name          string
	ctx           context.Context
	cancelContext context.CancelFunc
	rpcConn       *grpc.ClientConn
	cli           pb.DataMoverClient
	movers        []*DataMoverClient
	fsClient      fsroot.Client
	config        *pluginConfig
}

// Plugin represents a data mover plugin
type Plugin interface {
	AddMover(*Config)
	Run()
	Stop()
	Close() error
	Base() string
	FsName() string
	ConfigFile() string
}

func unixDialer(addr string, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("unix", addr, timeout)
}

// New returns a new *Plugin, or error
func New(name string, initClient func(string) (fsroot.Client, error)) (Plugin, error) {
	config := mustInitConfig()

	fsClient, err := initClient(config.ClientRoot)
	if err != nil {
		return nil, errors.Wrap(err, "client init failed")
	}

	ctx, cancel := context.WithCancel(context.Background())
	conn, err := grpc.Dial(config.AgentAddress, grpc.WithDialer(unixDialer), grpc.WithInsecure())
	if err != nil {
		return nil, errors.Wrap(err, "dial gprc server failed")
	}
	return &dmPlugin{
		name:          name,
		rpcConn:       conn,
		ctx:           ctx,
		cancelContext: cancel,
		cli:           pb.NewDataMoverClient(conn),
		fsClient:      fsClient,
		config:        config,
	}, nil
}

// FsName returns the associated Lustre filesystem name
func (a *dmPlugin) FsName() string {
	return a.fsClient.FsName()
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
	a.movers = append(a.movers, dm)
}

func (a *dmPlugin) Run() {
	var wg sync.WaitGroup
	for _, dm := range a.movers {
		wg.Add(1)
		go func(dm *DataMoverClient) {
			dm.Run(a.ctx)
			wg.Done()
		}(dm)
	}
	wg.Wait()
}

// Stop signals to all registered data movers that they should stop processing
// and shut down
func (a *dmPlugin) Stop() {
	a.cancelContext()
}

// Close closes the connection to the agent
func (a *dmPlugin) Close() error {
	return errors.Wrap(a.rpcConn.Close(), "closed failed")
}
