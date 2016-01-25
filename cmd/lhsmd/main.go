package main

import (
	"flag"
	"log"

	"github.intel.com/hpdd/applog"
	"github.intel.com/hpdd/liblog"
	"github.intel.com/hpdd/policy/pdm"
	"github.intel.com/hpdd/policy/pdm/lhsmd/agent"

	_ "github.intel.com/hpdd/policy/pdm/lhsmd/transport/grpc"
	// _ "github.intel.com/hpdd/policy/pdm/lhsmd/transport/queue"
)

var enableDebug bool

func init() {
	flag.BoolVar(&enableDebug, "debug", false, "enable debug output")
}

func main() {
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	if enableDebug {
		applog.SetLevel(applog.DEBUG)
		liblog.Enable()
	}
	liblog.SetWriter(applog.Writer())

	conf := pdm.ConfigInitMust()

	log.Printf("current configuration:\n%v", conf.String())

	agent.Daemon(conf)
}
