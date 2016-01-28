package main

import (
	"flag"
	"log"

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

	if err := agent.Daemon(conf); err != nil {
		svclog.Fail("Error in agent.Daemon(): %s", err)
	}
}
