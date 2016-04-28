package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"

	"github.com/hashicorp/hcl"

	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/policy/pdm/dmplugin"
	"github.intel.com/hpdd/policy/pdm/lhsm-plugin-posix/posix"
	"github.intel.com/hpdd/policy/pdm/lhsmd/config"
	"github.intel.com/hpdd/policy/pkg/client"
)

type (
	archiveConfig struct {
		Name string `hcl:",key"`
		ID   int    `hcl:"id"`
		Root string `hcl:"root"`
	}

	archiveSet []*archiveConfig

	posixConfig struct {
		AgentAddress string
		ClientRoot   string
		NumThreads   int        `hcl:"num_threads"`
		Archives     archiveSet `hcl:"archive"`
	}
)

func (a *archiveConfig) String() string {
	return fmt.Sprintf("%d:%s", a.ID, a.Root)
}

func (a *archiveConfig) checkValid() error {
	var errors []string

	if a.Root == "" {
		errors = append(errors, fmt.Sprintf("Archive %s: archive root not set", a.Name))
	}

	if a.ID < 1 {
		errors = append(errors, fmt.Sprintf("Archive %s: archive id not set", a.Name))
	}

	if len(errors) > 0 {
		return fmt.Errorf("Errors: %s", strings.Join(errors, ", "))
	}

	return nil
}

func (c *posixConfig) Merge(other *posixConfig) *posixConfig {
	result := new(posixConfig)

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

	result.Archives = c.Archives
	if len(other.Archives) > 0 {
		result.Archives = other.Archives
	}

	return result
}

func getAgentEnvSetting(name string) (value string) {
	if value = os.Getenv(name); value == "" {
		alert.Fatal("This plugin is intended to be launched by the agent.")
	}
	return
}

func start(cfg *posixConfig) {
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
		plugin.AddMover(&dmplugin.Config{
			Mover:      posix.NewMover(c, a.Root, uint32(a.ID)),
			NumThreads: 4,
			ArchiveID:  uint32(a.ID),
			FsName:     c.FsName(),
		})
	}

	<-done
	plugin.Stop()
}

func loadConfig(cfgFile string) (*posixConfig, error) {
	data, err := ioutil.ReadFile(cfgFile)
	if err != nil {
		return nil, err
	}

	cfg := new(posixConfig)
	if err := hcl.Decode(cfg, string(data)); err != nil {
		return nil, err
	}

	return cfg, nil
}

func getMergedConfig() (*posixConfig, error) {
	baseCfg := &posixConfig{
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

	start(cfg)
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
