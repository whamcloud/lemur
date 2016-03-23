package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/hashicorp/hcl"
	"github.com/rcrowley/go-metrics"

	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/audit"
	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/policy/pdm/dmplugin"
	"github.intel.com/hpdd/policy/pdm/lhsmd/agent"
	"github.intel.com/hpdd/policy/pkg/client"
)

type (
	archiveConfig struct {
		Name   string `hcl:",key"`
		ID     int
		Bucket string
		Prefix string
	}

	archiveSet []*archiveConfig

	s3Config struct {
		AgentAddress string
		ClientRoot   string
		Archives     archiveSet `hcl:"archive"`
	}
)

var rate metrics.Meter

func (a *archiveConfig) checkValid() error {
	var errors []string

	if a.Bucket == "" {
		errors = append(errors, fmt.Sprintf("Archive %s: bucket not set", a.Name))
	}

	if a.ID < 1 {
		errors = append(errors, fmt.Sprintf("Archive %s: archive id not set", a.Name))

	}

	if len(errors) > 0 {
		return fmt.Errorf("Errors: %s", strings.Join(errors, ", "))
	}

	return nil
}

func init() {
	rate = metrics.NewMeter()

	if debug.Enabled() {
		go func() {
			for {
				audit.Logf("total %s (1 min/5 min/15 min/inst): %s/%s/%s/%s msg/sec\n",
					humanize.Comma(rate.Count()),
					humanize.Comma(int64(rate.Rate1())),
					humanize.Comma(int64(rate.Rate5())),
					humanize.Comma(int64(rate.Rate15())),
					humanize.Comma(int64(rate.RateMean())),
				)
				time.Sleep(10 * time.Second)
			}
		}()
	}
}

func getAgentEnvSetting(name string) (value string) {
	if value = os.Getenv(name); value == "" {
		alert.Fatal("This plugin is intended to be launched by the agent.")
	}
	return
}

func s3(config *s3Config) {
	c, err := client.New(config.ClientRoot)
	if err != nil {
		alert.Fatal(err)
	}

	done := make(chan struct{})
	interruptHandler(func() {
		close(done)
	})

	plugin, err := dmplugin.New(config.AgentAddress)
	if err != nil {
		alert.Fatalf("failed to dial: %s", err)
	}
	defer plugin.Close()

	for _, a := range config.Archives {
		plugin.AddMover(S3Mover(c, uint32(a.ID), a.Bucket, a.Prefix))
	}

	<-done
	plugin.Stop()
}

func loadConfig(cfg *s3Config) error {
	cfgFile := path.Join(getAgentEnvSetting(agent.ConfigDirEnvVar), path.Base(os.Args[0]))

	data, err := ioutil.ReadFile(cfgFile)
	if err != nil {
		return err
	}

	return hcl.Decode(cfg, string(data))
}

func main() {
	cfg := &s3Config{
		AgentAddress: getAgentEnvSetting(agent.AgentConnEnvVar),
		ClientRoot:   getAgentEnvSetting(agent.PluginMountpointEnvVar),
	}

	if err := loadConfig(cfg); err != nil {
		alert.Fatalf("Failed to load config: %s", err)
	}

	if len(cfg.Archives) == 0 {
		alert.Fatalf("Invalid configuration: No archives defined")
	}

	for _, archive := range cfg.Archives {
		debug.Print(archive)
		if err := archive.checkValid(); err != nil {
			alert.Fatalf("Invalid configuration: %s", err)
		}
	}

	s3(cfg)
}

func interruptHandler(once func()) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGQUIT, syscall.SIGTERM)

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
