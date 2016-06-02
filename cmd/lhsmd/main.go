package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/rcrowley/go-metrics"
	"github.com/vrischmann/go-metrics-influxdb"

	"golang.org/x/net/context"

	"github.intel.com/hpdd/lemur/cmd/lhsmd/agent"
	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/audit"
	"github.intel.com/hpdd/logging/debug"

	// Register the supported transports
	_ "github.intel.com/hpdd/lemur/cmd/lhsmd/transport/grpc"
)

func init() {
	flag.Var(debug.FlagVar())
}

func interruptHandler(once func()) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		stopping := false
		for sig := range c {
			debug.Printf("signal received: %s", sig)
			if !stopping {
				stopping = true
				once()
			}
		}
	}()

}

func main() {
	flag.Parse()

	if debug.Enabled() {
		// Set this so that plugins can use it without needing
		// to mess around with plugin args.
		os.Setenv(debug.EnableEnvVar, "true")
	}

	// Setting the prefix helps us to track down deprecated calls to log.*
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetOutput(audit.Writer().Prefix("DEPRECATED "))

	conf := agent.ConfigInitMust()

	debug.Printf("current configuration:\n%v", conf.String())
	if err := agent.ConfigureMounts(conf); err != nil {
		alert.Abort(errors.Wrap(err, "Error while creating Lustre mountpoints"))
	}

	if conf.InfluxDB != nil && conf.InfluxDB.URL != "" {
		debug.Print("Configuring InfluxDB stats target")
		go influxdb.InfluxDB(
			metrics.DefaultRegistry, // metrics registry
			time.Second*10,          // interval
			conf.InfluxDB.URL,
			conf.InfluxDB.DB,       // your InfluxDB database
			conf.InfluxDB.User,     // your InfluxDB user
			conf.InfluxDB.Password, // your InfluxDB password
		)
	}

	ct, err := agent.New(conf)
	if err != nil {
		alert.Abort(errors.Wrap(err, "Error creating agent"))
	}

	interruptHandler(func() {
		ct.Stop()
	})

	if err := ct.Start(context.Background()); err != nil {
		alert.Abort(errors.Wrap(err, "Error in HsmAgent.Start()"))
	}

	if err := agent.CleanupMounts(conf); err != nil {
		alert.Abort(errors.Wrap(err, "Error while cleaning up Lustre mountpoints"))
	}
}
