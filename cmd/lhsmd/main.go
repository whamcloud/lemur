package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rcrowley/go-metrics"
	"github.com/vrischmann/go-metrics-influxdb"

	"golang.org/x/net/context"

	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/audit"
	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/policy/pdm/lhsmd/agent"
	"github.intel.com/hpdd/policy/pkg/client"

	// Register the supported transports
	_ "github.intel.com/hpdd/policy/pdm/lhsmd/transport/grpc"
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
		alert.Fatalf("Error while creating Lustre mountpoints: %s", err)
	}

	client, err := client.New(conf.AgentMountpoint)
	if err != nil {
		alert.Fatalf("Error while create Lustre client: %s", err)
	}
	ct, err := agent.New(conf, client)
	if err != nil {
		alert.Fatalf("Error creating agent: %s", err)
	}

	if conf.InfluxDB != nil {
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
	ctx, cancel := context.WithCancel(context.Background())
	interruptHandler(func() {
		ct.Stop()
		cancel()
	})

	if err := ct.Start(ctx); err != nil {
		alert.Fatalf("Error in HsmAgent.Start(): %s", err)
	}

	if err := agent.CleanupMounts(conf); err != nil {
		alert.Warnf("Error while cleaning up Lustre mountpoints: %s", err)
	}
}
