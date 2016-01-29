package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/rcrowley/go-metrics"
	"google.golang.org/grpc"

	"github.intel.com/hpdd/policy/pdm"
	"github.intel.com/hpdd/policy/pdm/dmplugin"
	pb "github.intel.com/hpdd/policy/pdm/pdm"
	"github.intel.com/hpdd/policy/pkg/client"
	"github.intel.com/hpdd/svclog"
)

var (
	rate        metrics.Meter
	enableDebug bool
)

func init() {
	rate = metrics.NewMeter()

	flag.BoolVar(&enableDebug, "debug", false, "Enable debug logging")
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

func posix(cli pb.DataMoverClient, conf *pdm.HSMConfig) {
	var movers []*dmplugin.DataMoverClient
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
			mover := NewMover(a.Name, c, a.PosixDir, a.ArchiveID)
			dm := dmplugin.New(cli, mover)
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
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	conf := pdm.ConfigInitMust()
	flag.Parse()

	if enableDebug {
		svclog.EnableDebug()
	}

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
