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

	if enableDebug {
		applog.SetLevel(applog.DEBUG)
		liblog.Enable()
	}
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	// Setting the prefix helps us to track down deprecated calls to log.*
	log.SetOutput(applog.Writer().Prefix("DEPRECATED"))
	liblog.SetWriter(applog.Writer())

	conf := pdm.ConfigInitMust()

	applog.Debug("current configuration:\n%v", conf.String())

	if err := agent.Daemon(conf); err != nil {
		applog.Fail("Error in agent.Daemon(): %s", err)
	}
}
