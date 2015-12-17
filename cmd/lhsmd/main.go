package main

import (
	"flag"
	"log"
	"os"

	"github.intel.com/hpdd/policy/pdm"
	"github.intel.com/hpdd/policy/pdm/lhsmd/agent"
	"github.intel.com/hpdd/policy/pkg/workq"

	_ "github.intel.com/hpdd/policy/pdm/lhsmd/transport/grpc"
	// _ "github.intel.com/hpdd/policy/pdm/lhsmd/transport/queue"
)

func main() {
	var reset, trace bool

	flag.BoolVar(&reset, "reset", false, "Reset queue")
	flag.BoolVar(&trace, "trace", false, "Print redis trace")

	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Llongfile)

	conf := pdm.ConfigInitMust()
	if reset {
		workq.MasterReset("pdm", conf.RedisServer)
		os.Exit(0)
	}

	log.Printf("current configuration:\n%v", conf.String())

	agent.Daemon(conf)
}
