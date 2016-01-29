package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/net/context"

	"github.intel.com/hpdd/policy/pdm"
	"github.intel.com/hpdd/policy/pdm/lhsmd/agent"
	"github.intel.com/hpdd/svclog"

	_ "github.intel.com/hpdd/policy/pdm/lhsmd/transport/grpc"
	// _ "github.intel.com/hpdd/policy/pdm/lhsmd/transport/queue"
)

var enableDebug bool

func init() {
	flag.BoolVar(&enableDebug, "debug", false, "enable debug output")
}

func interruptHandler(once func()) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGQUIT, syscall.SIGTERM)

	go func() {
		stopping := false
		for sig := range c {
			svclog.Debug("signal received: %s", sig)
			if !stopping {
				stopping = true
				once()
			}
		}
	}()

}

func main() {
	flag.Parse()

	if enableDebug {
		svclog.EnableDebug()
	}
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	// Setting the prefix helps us to track down deprecated calls to log.*
	log.SetOutput(svclog.Writer().Prefix("DEPRECATED"))

	conf := pdm.ConfigInitMust()

	svclog.Debug("current configuration:\n%v", conf.String())

	ct, err := agent.New(conf)
	if err != nil {
		svclog.Fail("Error creating agent: %s", err)
	}

	interruptHandler(func() {
		ct.Stop()
	})

	ctx := context.Background()
	if err := ct.Start(ctx); err != nil {
		svclog.Fail("Error in HsmAgent.Start(): %s", err)
	}
}
