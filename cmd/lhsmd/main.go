package main

import (
	"flag"
	"log"

	"github.intel.com/hpdd/policy/pdm"
	"github.intel.com/hpdd/policy/pdm/lhsmd/agent"

	_ "github.intel.com/hpdd/policy/pdm/lhsmd/transport/grpc"
	// _ "github.intel.com/hpdd/policy/pdm/lhsmd/transport/queue"
)

func main() {
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	conf := pdm.ConfigInitMust()

	log.Printf("current configuration:\n%v", conf.String())

	agent.Daemon(conf)
}
