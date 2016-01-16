package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rcrowley/go-metrics"

	"github.intel.com/hpdd/policy/pdm"
	pb "github.intel.com/hpdd/policy/pdm/pdm"
	"github.intel.com/hpdd/policy/pkg/client"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	rate metrics.Meter
)

type PosixDataMover struct {
	rpcClient  pb.DataMoverClient
	client     *client.Client
	archiveDir string
	archiveID  uint32
	stop       chan struct{}
}

func init() {
	rate = metrics.NewMeter()

	/*
		go func() {
			for {
				fmt.Fprintf(os.Stderr, "total %s msg/sec (1 min/5 min/15 min/inst): %s/%s/%s/%s\r",
					humanize.Comma(rate.Count()),
					humanize.Comma(int64(rate.Rate1())),
					humanize.Comma(int64(rate.Rate5())),
					humanize.Comma(int64(rate.Rate15())),
					humanize.Comma(int64(rate.RateMean())),
				)
				time.Sleep(1 * time.Second)
			}
		}()
	*/
}

type key int

var handleKey key = 0

func withHandle(ctx context.Context, handle *pb.Handle) context.Context {
	return context.WithValue(ctx, handleKey, handle)
}

func getHandle(ctx context.Context) (*pb.Handle, bool) {
	handle, ok := ctx.Value(handleKey).(*pb.Handle)
	return handle, ok
}

func (dm *PosixDataMover) Run() {
	ctx, cancel := context.WithCancel(context.Background())

	handle, err := dm.registerEndpoint(ctx)
	if err != nil {
		log.Fatal(err)
	}
	ctx = withHandle(ctx, handle)
	actions := dm.processActions(ctx)
	status := dm.processStatus(ctx)

	for i := 0; i < 2; i++ {
		go dm.handler(fmt.Sprintf("handler-%d", i), actions, status)
	}

	<-dm.stop
	cancel()
	close(status)
}

func (dm *PosixDataMover) Stop() {
	close(dm.stop)
}

func (dm *PosixDataMover) registerEndpoint(ctx context.Context) (*pb.Handle, error) {

	handle, err := dm.rpcClient.Register(ctx, &pb.Endpoint{
		FsUrl:   dm.client.FsName(),
		Archive: dm.archiveID,
	})
	if err != nil {
		return nil, err
	}
	log.Printf("Registered archive %d,  cookie %x\n\n", dm.archiveID, handle.Id)
	return handle, nil
}

func (dm *PosixDataMover) processActions(ctx context.Context) chan *pb.ActionItem {
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
			log.Printf("Got message %x op: %v %v\n", action.Cookie, action.Op, action.PrimaryPath)

			actions <- action
		}
	}()

	return actions

}

func (dm *PosixDataMover) processStatus(ctx context.Context) chan *pb.ActionStatus {
	status := make(chan *pb.ActionStatus)
	go func() {
		handle, ok := getHandle(ctx)
		if !ok {
			log.Fatal("No context")
		}

		acks, err := dm.rpcClient.StatusStream(ctx)
		if err != nil {
			log.Fatalf("StatusStream() failed: %v", err)
		}
		for reply := range status {
			reply.Handle = handle
			log.Printf("Sent reply  %x error: %#v\n", reply.Cookie, reply.Error)
			err := acks.Send(reply)
			if err != nil {
				log.Fatalf("Failed to ack message %x: %v", reply.Cookie, err)
			}
		}
	}()
	return status
}

func (dm *PosixDataMover) handler(name string, actions chan *pb.ActionItem, status chan *pb.ActionStatus) {
	for action := range actions {
		switch action.Op {
		case pb.Command_ARCHIVE:
			log.Printf("%s: %v not implemented: %x\n", name, action.Op, action.Cookie)

		case pb.Command_RESTORE:
			log.Printf("%s: %v not implemented: %x\n", name, action.Op, action.Cookie)

		case pb.Command_REMOVE:
			log.Printf("%s: %v not implemented: %x\n", name, action.Op, action.Cookie)

		case pb.Command_CANCEL:
			log.Printf("%s: %v not implemented: %x\n", name, action.Op, action.Cookie)
		}
		time.Sleep((time.Duration(rand.Intn(3)) + 1) * time.Second)
		rate.Mark(1)
		status <- &pb.ActionStatus{Cookie: action.Cookie, Completed: true, Error: 1}
	}
	log.Printf("%s: stopping\n", name)
}

func posix(cli pb.DataMoverClient, conf *pdm.HSMConfig) {
	var movers []*PosixDataMover
	c, err := client.New(conf.Lustre)
	if err != nil {
		log.Fatal(err)
	}

	done := make(chan struct{})
	interruptHandler(func() {
		close(done)
	})

	for _, a := range conf.Archives {
		if a.Type == "posix" {
			dm := &PosixDataMover{
				rpcClient:  cli,
				client:     c,
				archiveID:  a.ArchiveID,
				archiveDir: a.PosixDir,
				stop:       make(chan struct{}),
			}
			go dm.Run()
			movers = append(movers, dm)
		}
	}
	<-done
	for _, dm := range movers {
		dm.Stop()
	}
}

func main() {
	conf := pdm.ConfigInitMust()
	flag.Parse()

	conn, err := grpc.Dial("localhost:4242", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()
	cli := pb.NewDataMoverClient(conn)

	posix(cli, conf)
}

func interruptHandler(once func()) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGQUIT, syscall.SIGTERM)

	go func() {
		stopping := false
		for sig := range c {
			log.Println("signal received:", sig)
			if !stopping {
				stopping = true
				once()
			}
		}
	}()
}
