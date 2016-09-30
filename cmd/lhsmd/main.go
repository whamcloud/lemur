package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/intel-hpdd/go-metrics-influxdb"
	"github.com/pkg/errors"
	"github.com/rcrowley/go-metrics"

	"golang.org/x/net/context"

	"github.intel.com/hpdd/lemur/cmd/lhsmd/agent"
	"github.intel.com/hpdd/lemur/pkg/fsroot"
	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/audit"
	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/lustre/hsm"

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

func run(conf *agent.Config) error {
	debug.Printf("current configuration:\n%v", conf.String())
	if err := agent.ConfigureMounts(conf); err != nil {
		return errors.Wrap(err, "Error while creating Lustre mountpoints")
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

	client, err := fsroot.New(conf.AgentMountpoint())
	if err != nil {
		return errors.Wrap(err, "Could not get fs client")
	}
	as := hsm.NewActionSource(client.Root())

	ct, err := agent.New(conf, client, as)
	if err != nil {
		return errors.Wrap(err, "Error creating agent")
	}

	interruptHandler(func() {
		ct.Stop()
	})

	return errors.Wrap(ct.Start(context.Background()),
		"Error in HsmAgent.Start()")
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
	err := run(conf)

	// Ensure that we always clean up.
	if err := agent.CleanupMounts(conf); err != nil {
		alert.Warn(errors.Wrap(err, "Error while cleaning up Lustre mountpoints"))
	}

	if err != nil {
		alert.Abort(err)
	}
}
