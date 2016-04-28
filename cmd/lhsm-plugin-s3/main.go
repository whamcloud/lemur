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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/dustin/go-humanize"
	"github.com/hashicorp/hcl"
	"github.com/rcrowley/go-metrics"

	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/audit"
	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/policy/pdm/dmplugin"
	"github.intel.com/hpdd/policy/pdm/lhsmd/config"
	"github.intel.com/hpdd/policy/pkg/client"
)

type (
	archiveConfig struct {
		Name   string `hcl:",key"`
		ID     int
		Region string
		Bucket string
		Prefix string
	}

	archiveSet []*archiveConfig

	s3Config struct {
		AgentAddress string
		ClientRoot   string
		NumThreads   int        `hcl:"num_threads"`
		Region       string     `hcl:"region"`
		Archives     archiveSet `hcl:"archive"`
	}
)

// Should this be configurable?
const updateInterval = 10 * time.Second

var rate metrics.Meter

func (a *archiveConfig) String() string {
	return fmt.Sprintf("%d:%s:%s/%s", a.ID, a.Region, a.Bucket, a.Prefix)
}

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

func (c *s3Config) Merge(other *s3Config) *s3Config {
	result := new(s3Config)

	result.AgentAddress = c.AgentAddress
	if other.AgentAddress != "" {
		result.AgentAddress = other.AgentAddress
	}

	result.ClientRoot = c.ClientRoot
	if other.ClientRoot != "" {
		result.ClientRoot = other.ClientRoot
	}

	result.NumThreads = c.NumThreads
	if other.NumThreads > 0 {
		result.NumThreads = other.NumThreads
	}

	result.Region = c.Region
	if other.Region != "" {
		result.Region = other.Region
	}

	result.Archives = c.Archives
	if len(other.Archives) > 0 {
		result.Archives = other.Archives
	}

	return result
}

func init() {
	rate = metrics.NewMeter()

	// if debug.Enabled() {
	go func() {
		var lastCount int64
		for {
			if lastCount != rate.Count() {
				audit.Logf("total %s (1 min/5 min/15 min/inst): %s/%s/%s/%s msg/sec\n",
					humanize.Comma(rate.Count()),
					humanize.Comma(int64(rate.Rate1())),
					humanize.Comma(int64(rate.Rate5())),
					humanize.Comma(int64(rate.Rate15())),
					humanize.Comma(int64(rate.RateMean())),
				)
				lastCount = rate.Count()
			}
			time.Sleep(10 * time.Second)
		}
	}()
	// }
}

func getAgentEnvSetting(name string) (value string) {
	if value = os.Getenv(name); value == "" {
		alert.Fatal("This plugin is intended to be launched by the agent.")
	}
	return
}

func s3Svc(region string) *s3.S3 {
	// TODO: Allow more per-archive configuration options?
	return s3.New(session.New(aws.NewConfig().WithRegion(region)))
}

func loadConfig(cfgFile string) (*s3Config, error) {
	data, err := ioutil.ReadFile(cfgFile)
	if err != nil {
		return nil, err
	}

	cfg := new(s3Config)
	if err := hcl.Decode(cfg, string(data)); err != nil {
		return nil, err
	}

	return cfg, nil
}

func getMergedConfig() (*s3Config, error) {
	baseCfg := &s3Config{
		Region:       "us-east-1",
		AgentAddress: getAgentEnvSetting(config.AgentConnEnvVar),
		ClientRoot:   getAgentEnvSetting(config.PluginMountpointEnvVar),
	}

	cfgFile := path.Join(getAgentEnvSetting(config.ConfigDirEnvVar), path.Base(os.Args[0]))
	cfg, err := loadConfig(cfgFile)
	if err != nil {
		return nil, fmt.Errorf("Failed to load config: %s", err)
	}

	return baseCfg.Merge(cfg), nil
}

func main() {
	cfg, err := getMergedConfig()
	if err != nil {
		alert.Fatalf("Unable to determine plugin configuration: %s", err)
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

	c, err := client.New(cfg.ClientRoot)
	if err != nil {
		alert.Fatal(err)
	}

	done := make(chan struct{})
	interruptHandler(func() {
		close(done)
	})

	plugin, err := dmplugin.New(cfg.AgentAddress)
	if err != nil {
		alert.Fatalf("failed to dial: %s", err)
	}
	defer plugin.Close()

	for _, a := range cfg.Archives {
		// Allow each archive to set its own region, but default
		// to the region set for the plugin.
		region := cfg.Region
		if a.Region != "" {
			region = a.Region
		}
		s3Svc := s3Svc(region)
		plugin.AddMover(&dmplugin.Config{
			Mover:      S3Mover(c, s3Svc, uint32(a.ID), a.Bucket, a.Prefix),
			NumThreads: 4,
			FsName:     c.FsName(),
			ArchiveID:  uint32(a.ID),
		})
	}

	<-done
	plugin.Stop()
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
