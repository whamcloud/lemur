package main

import (
	"flag"
	"fmt"
	"io"
	"log"
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

	acks, err := client.StatusStream(ctx)
	if err != nil {
		log.Fatalf("StatusStream() failed: %v", err)
	}

	for {
		action, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Fatalf("Failed to receive a message: %v", err)
		}
		fmt.Printf("\nGot message %x op: %d\n", action.Cookie, action.Op)
		rate.Mark(1)
		err = acks.Send(&pb.ActionStatus{Cookie: action.Cookie, Completed: true, Error: 1, Handle: handle})
		if err != nil {
			log.Fatalf("Failed to ack message %x: %v", action.Cookie, err)
		}
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
