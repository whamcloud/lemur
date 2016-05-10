package dmplugin

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"syscall"

	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/debug"
	pb "github.intel.com/hpdd/policy/pdm/pdm"
	"golang.org/x/net/context"
)

type (
	// DataMoverClient is the data mover client to the HSM agent
	DataMoverClient struct {
		plugin    *dmPlugin
		rpcClient pb.DataMoverClient
		stop      chan struct{}
		status    chan *pb.ActionStatus
		mover     Mover
		config    *Config
	}

	// Configuration for DatamMoverClient
	Config struct {
		Mover      Mover
		NumThreads int
		ArchiveID  uint32
	}

	// Action is a data movement action
	dmAction struct {
		status       chan *pb.ActionStatus
		item         *pb.ActionItem
		fileID       []byte
		actualLength *uint64
	}

	Action interface {
		// Update sends an action status update
		Update(offset, length, max uint64) error
		// Complete signals that the action has completed
		Complete() error
		// Fail signals that the action has failed
		Fail(err error) error
		// ID returns the action item's ID
		ID() uint64
		// Offset returns the current offset of the action item
		Offset() uint64
		// Length returns the expected length of the action item's file
		Length() uint64
		// Data returns a byte slice of the action item's data
		Data() []byte
		// PrimaryPath returns the action item's primary file path
		PrimaryPath() string

		// WritePath returns the action item's write path (e.g. for restores)
		WritePath() string
		// FileID returns the action item's file id
		FileID() []byte

		// SetFileID sets the action's file id
		SetFileID(id []byte)

		// SetActualLength sets the action's actual file length
		SetActualLength(length uint64)
	}

	// Mover defines an interface for data mover implementations
	Mover interface {
	}

	// Archiver defines an interface for data movers capable of
	// fulfilling Archive requests
	Archiver interface {
		Archive(Action) error
	}

	// Restorer defines an interface for data movers capable of
	// fulfilling Restore requests
	Restorer interface {
		Restore(Action) error
	}

	// Remover defines an interface for data movers capable of
	// fulfilling Remove requests
	Remover interface {
		Remove(Action) error
	}
)

type key int

var handleKey key

const (
	defaultNumThreads = 4
)

func withHandle(ctx context.Context, handle *pb.Handle) context.Context {
	return context.WithValue(ctx, handleKey, handle)
}

func getHandle(ctx context.Context) (*pb.Handle, bool) {
	handle, ok := ctx.Value(handleKey).(*pb.Handle)
	return handle, ok
}

// Update sends an action status update
func (a *dmAction) Update(offset, length, max uint64) error {
	a.status <- &pb.ActionStatus{
		Id:     a.item.Id,
		Offset: offset,
		Length: length,
	}
	return nil
}

// Complete signals that the action has completed
func (a *dmAction) Complete() error {
	status := &pb.ActionStatus{
		Id:        a.item.Id,
		Completed: true,
		Offset:    a.item.Offset,
		Length:    a.item.Length,
		FileId:    a.fileID,
	}
	if a.actualLength != nil {
		status.Length = *a.actualLength
	}
	a.status <- status
	return nil
}

func getErrno(err error) int32 {
	if errno, ok := err.(syscall.Errno); ok {
		return int32(errno)
	}
	return -1
}

// Fail signals that the action has failed
func (a *dmAction) Fail(err error) error {
	alert.Warnf("fail: id:%d %v", a.item.Id, err)
	a.status <- &pb.ActionStatus{
		Id:        a.item.Id,
		Completed: true,

		Error: getErrno(err),
	}
	return nil
}

// ID returns the action item's ID
func (a *dmAction) ID() uint64 {
	return a.item.Id
}

// Offset returns the current offset of the action item
func (a *dmAction) Offset() uint64 {
	return a.item.Offset
}

// Length returns the expected length of the action item's file
func (a *dmAction) Length() uint64 {
	return a.item.Length
}

// Data returns a byte slice of the action item's data
func (a *dmAction) Data() []byte {
	return a.item.Data
}

// PrimaryPath returns the action item's primary file path
func (a *dmAction) PrimaryPath() string {
	return a.item.PrimaryPath
}

// WritePath returns the action item's write path (e.g. for restores)
func (a *dmAction) WritePath() string {
	return a.item.WritePath
}

// FileID returns the action item's file id
func (a *dmAction) FileID() []byte {
	return a.item.FileId
}

// SetFileID sets the action's file id
func (a *dmAction) SetFileID(id []byte) {
	a.fileID = id
}

// SetActualLength sets the action's actual file length
func (a *dmAction) SetActualLength(length uint64) {
	a.actualLength = &length
}

// NewMover returns a new *DataMoverClient
func NewMover(plugin *dmPlugin, cli pb.DataMoverClient, config *Config) *DataMoverClient {
	return &DataMoverClient{
		plugin:    plugin,
		rpcClient: cli,
		mover:     config.Mover,
		stop:      make(chan struct{}),
		status:    make(chan *pb.ActionStatus, 2),
		config:    config,
	}
}

// Run begins listening for and processing incoming action items
func (dm *DataMoverClient) Run() {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	handle, err := dm.registerEndpoint(ctx)
	if err != nil {
		alert.Fatal(err)
	}
	ctx = withHandle(ctx, handle)
	actions := dm.processActions(ctx)
	dm.processStatus(ctx)

	n := defaultNumThreads
	if dm.config.NumThreads > 0 {
		n = dm.config.NumThreads
	}

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			dm.handler(fmt.Sprintf("handler-%d", i), actions)
			wg.Done()
		}(i)
		dm.processStatus(ctx)
	}

	<-dm.stop
	debug.Printf("Shutting down Data Mover")
	cancel()
	wg.Wait()
	close(dm.status)
}

// Stop signals to the client that it should stop processing and shut down
func (dm *DataMoverClient) Stop() {
	close(dm.stop)
}

func (dm *DataMoverClient) registerEndpoint(ctx context.Context) (*pb.Handle, error) {

	handle, err := dm.rpcClient.Register(ctx, &pb.Endpoint{
		FsUrl:   dm.plugin.fsClient.FsName(),
		Archive: dm.config.ArchiveID,
	})
	if err != nil {
		return nil, err
	}
	debug.Printf("Registered archive %d,  cookie %x", dm.config.ArchiveID, handle.Id)
	return handle, nil
}

func (dm *DataMoverClient) processActions(ctx context.Context) chan *pb.ActionItem {
	actions := make(chan *pb.ActionItem)

	go func() {
		handle, ok := getHandle(ctx)
		if !ok {
			alert.Fatal("No context")
		}
		stream, err := dm.rpcClient.GetActions(ctx, handle)
		if err != nil {
			alert.Fatalf("GetActions() failed: %v", err)
		}
		for {
			action, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				close(actions)
				alert.Fatalf("Failed to receive a message: %v", err)
			}
			// debug.Printf("Got message id:%d op: %v %v", action.Id, action.Op, action.PrimaryPath)

			actions <- action
		}
	}()

	return actions

}

func (dm *DataMoverClient) processStatus(ctx context.Context) {
	go func() {
		handle, ok := getHandle(ctx)
		if !ok {
			alert.Fatal("No context")
		}

		acks, err := dm.rpcClient.StatusStream(ctx)
		if err != nil {
			alert.Fatalf("StatusStream() failed: %v", err)
		}
		for reply := range dm.status {
			reply.Handle = handle
			// debug.Printf("Sent reply  %x error: %#v", reply.Id, reply.Error)
			err := acks.Send(reply)
			if err != nil {
				alert.Fatalf("Failed to ack message %x: %v", reply.Id, err)
			}
		}
	}()
	return
}

func (dm *DataMoverClient) handler(name string, actions chan *pb.ActionItem) {
	for item := range actions {
		var ret error
		action := &dmAction{
			status: dm.status,
			item:   item,
		}

		ret = errors.New("Command not supported")

		switch item.Op {
		case pb.Command_ARCHIVE:
			if archiver, ok := dm.mover.(Archiver); ok {
				ret = archiver.Archive(action)
			}
		case pb.Command_RESTORE:
			if restorer, ok := dm.mover.(Restorer); ok {
				ret = restorer.Restore(action)
			}
		case pb.Command_REMOVE:
			if remover, ok := dm.mover.(Remover); ok {
				ret = remover.Remove(action)
			}
		case pb.Command_CANCEL:
			// TODO: Cancel in-progress action using a context
		default:
			ret = errors.New("Unknown cmmand")
		}

		//		rate.Mark(1)
		// debug.Printf("completed (action: %v) %v ", action, ret)
		if ret != nil {
			action.Fail(ret)
		} else {
			action.Complete()
		}
	}
	debug.Printf("%s: stopping", name)
}
