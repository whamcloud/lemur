package dmplugin

import (
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"syscall"

	"github.intel.com/hpdd/logging/debug"
	pb "github.intel.com/hpdd/policy/pdm/pdm"
	"golang.org/x/net/context"
)

type (
	DataMoverClient struct {
		rpcClient pb.DataMoverClient
		stop      chan struct{}
		status    chan *pb.ActionStatus
		mover     Mover
	}

	Action struct {
		status       chan *pb.ActionStatus
		item         *pb.ActionItem
		fileId       []byte
		actualLength *uint64
	}

	Mover interface {
		FsName() string
		ArchiveID() uint32
	}

	Archiver interface {
		Archive(action *Action) error
	}

	Restorer interface {
		Restore(action *Action) error
	}

	Remover interface {
		Remove(action *Action) error
	}
)

type key int

var handleKey key = 0

func withHandle(ctx context.Context, handle *pb.Handle) context.Context {
	return context.WithValue(ctx, handleKey, handle)
}

func getHandle(ctx context.Context) (*pb.Handle, bool) {
	handle, ok := ctx.Value(handleKey).(*pb.Handle)
	return handle, ok
}

func (action *Action) Update(offset, length, max int64) error {
	action.status <- &pb.ActionStatus{
		Id:     action.item.Id,
		Offset: uint64(offset),
		Length: uint64(length),
	}
	return nil
}

func (action *Action) Complete() error {
	status := &pb.ActionStatus{
		Id:        action.item.Id,
		Completed: true,
		Offset:    action.item.Offset,
		Length:    action.item.Length,
		FileId:    action.fileId,
	}
	if action.actualLength != nil {
		status.Length = *action.actualLength
	}
	action.status <- status
	return nil
}

func getErrno(err error) int32 {
	if errno, ok := err.(syscall.Errno); ok {
		return int32(errno)
	}
	return -1
}

func (action *Action) Fail(err error) error {
	debug.Printf("fail: id:%d %v", action.item.Id, err)
	action.status <- &pb.ActionStatus{
		Id:        action.item.Id,
		Completed: true,

		Error: getErrno(err),
	}
	return nil
}

func (a *Action) ID() uint64 {
	return a.item.Id
}

func (a *Action) Offset() int64 {
	return int64(a.item.Offset)
}

func (a *Action) Length() int64 {
	return int64(a.item.Length)
}

func (a *Action) Data() []byte {
	return a.item.Data
}

func (a *Action) PrimaryPath() string {
	return a.item.PrimaryPath
}

func (a *Action) WritePath() string {
	return a.item.WritePath
}

func (a *Action) FileID() string {
	return string(a.item.FileId)
}

func (a *Action) SetFileID(id []byte) {
	a.fileId = id
}

func (a *Action) SetActualLength(length uint64) {
	a.actualLength = &length
}

func NewMover(cli pb.DataMoverClient, mover Mover) *DataMoverClient {
	return &DataMoverClient{
		rpcClient: cli,
		mover:     mover,
		stop:      make(chan struct{}),
		status:    make(chan *pb.ActionStatus, 2),
	}
}

func (dm *DataMoverClient) Run() {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	handle, err := dm.registerEndpoint(ctx)
	if err != nil {
		log.Fatal(err)
	}
	ctx = withHandle(ctx, handle)
	actions := dm.processActions(ctx)
	dm.processStatus(ctx)

	for i := 0; i < 2; i++ {
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

func (dm *DataMoverClient) Stop() {
	close(dm.stop)
}

func (dm *DataMoverClient) registerEndpoint(ctx context.Context) (*pb.Handle, error) {

	handle, err := dm.rpcClient.Register(ctx, &pb.Endpoint{
		FsUrl:   dm.mover.FsName(),
		Archive: dm.mover.ArchiveID(),
	})
	if err != nil {
		return nil, err
	}
	debug.Printf("Registered archive %d,  cookie %x", dm.mover.ArchiveID(), handle.Id)
	return handle, nil
}

func (dm *DataMoverClient) processActions(ctx context.Context) chan *pb.ActionItem {
	actions := make(chan *pb.ActionItem)

	go func() {
		handle, ok := getHandle(ctx)
		if !ok {
			log.Fatal("No context")
		}
		stream, err := dm.rpcClient.GetActions(ctx, handle)
		if err != nil {
			log.Fatalf("GetActions() failed: %v", err)
		}
		for {
			action, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				close(actions)
				log.Fatalf("Failed to receive a message: %v", err)
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
			log.Fatal("No context")
		}

		acks, err := dm.rpcClient.StatusStream(ctx)
		if err != nil {
			log.Fatalf("StatusStream() failed: %v", err)
		}
		for reply := range dm.status {
			reply.Handle = handle
			// debug.Printf("Sent reply  %x error: %#v", reply.Id, reply.Error)
			err := acks.Send(reply)
			if err != nil {
				log.Fatalf("Failed to ack message %x: %v", reply.Id, err)
			}
		}
	}()
	return
}

func (dm *DataMoverClient) handler(name string, actions chan *pb.ActionItem) {
	for item := range actions {
		var ret error
		action := &Action{
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
