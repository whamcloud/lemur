package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/rcrowley/go-metrics"

	pb "github.intel.com/hpdd/policy/pdm/pdm"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	rate    metrics.Meter
	archive uint
)

func init() {
	flag.UintVar(&archive, "archive", 1, "archive id")

	rate = metrics.NewMeter()

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

}

func handler(actions chan *pb.ActionItem, status chan *pb.ActionStatus) {
	for action := range actions {
		switch action.Op {
		case pb.Command_ARCHIVE:
			log.Printf("%v not implemented: %x\n", action.Op, action.Cookie)

		case pb.Command_RESTORE:
			log.Printf("%v not implemented: %x\n", action.Op, action.Cookie)

		case pb.Command_REMOVE:
			log.Printf("%v not implemented: %x\n", action.Op, action.Cookie)

		case pb.Command_CANCEL:
			log.Printf("%v not implemented: %x\n", action.Op, action.Cookie)
		}
		time.Sleep((time.Duration(rand.Intn(3)) + 1) * time.Second)
		rate.Mark(1)
		status <- &pb.ActionStatus{Cookie: action.Cookie, Completed: true, Error: 1}
	}
}

func noop(client pb.DataMoverClient) {
	ctx := context.Background()
	handle, err := client.Register(ctx, &pb.Endpoint{Archive: uint32(archive)})
	if err != nil {
		log.Fatalf("Register() failed: %v", err)

	}
	log.Printf("Archive %d,  cookie %x\n\n", archive, handle.Id)
	stream, err := client.GetActions(ctx, handle)
	if err != nil {
		log.Fatalf("GetActions() failed: %v", err)
	}

	actions := make(chan *pb.ActionItem)
	status := make(chan *pb.ActionStatus)
	go func() {
		acks, err := client.StatusStream(ctx)
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

	go handler(actions, status)
	go handler(actions, status)

	for {
		action, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			close(actions)
			close(status)
			log.Fatalf("Failed to receive a message: %v", err)
		}
		log.Printf("Got message %x op: %v %v\n", action.Cookie, action.Op, action.PrimaryPath)

		actions <- action
	}
}

func main() {
	flag.Parse()
	conn, err := grpc.Dial("localhost:4242", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()
	client := pb.NewDataMoverClient(conn)

	noop(client)
}
