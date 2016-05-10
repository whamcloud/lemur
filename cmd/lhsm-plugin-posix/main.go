package main

import (
	"fmt"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"

	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/policy/pdm/dmplugin"
	"github.intel.com/hpdd/policy/pdm/lhsm-plugin-posix/posix"
)

type (
	archiveConfig struct {
		Name      string          `hcl:",key"`
		ID        int             `hcl:"id"`
		Root      string          `hcl:"root"`
		Checksums *checksumConfig `hcl:"checksums"`
	}

	archiveSet []*archiveConfig

	checksumConfig struct {
		Disabled                bool `hcl:"disabled"`
		DisableCompareOnRestore bool `hcl:"disable_compare_on_restore"`
	}

	posixConfig struct {
		NumThreads int             `hcl:"num_threads"`
		Archives   archiveSet      `hcl:"archive"`
		Checksums  *checksumConfig `hcl:"checksums"`
	}
)

func (c *posixConfig) String() string {
	return dmplugin.DisplayConfig(c)
}

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

func (c *checksumConfig) Merge(other *checksumConfig) *checksumConfig {
	result := new(checksumConfig)

	// Just defer to the other config
	result.Disabled = other.Disabled
	result.DisableCompareOnRestore = other.DisableCompareOnRestore

	return result
}

func (c *checksumConfig) ToPosix() *posix.ChecksumConfig {
	return &posix.ChecksumConfig{
		Disabled:                c.Disabled,
		DisableCompareOnRestore: c.DisableCompareOnRestore,
	}
}

func (c *posixConfig) Merge(other *posixConfig) *posixConfig {
	result := new(posixConfig)

	result.NumThreads = c.NumThreads
	if other.NumThreads > 0 {
		result.NumThreads = other.NumThreads
	}

	result.Archives = c.Archives
	if len(other.Archives) > 0 {
		result.Archives = other.Archives
	}

	result.Checksums = c.Checksums
	if other.Checksums != nil {
		result.Checksums = result.Checksums.Merge(other.Checksums)
	}

	return result
}

func createMovers(cfg *posixConfig) (map[uint32]*posix.Mover, error) {
	movers := make(map[uint32]*posix.Mover)

	for _, a := range cfg.Archives {
		csc := cfg.Checksums // use global config by default
		if a.Checksums != nil {
			csc = a.Checksums
		}
		mover, err := posix.NewMover(&posix.MoverConfig{
			Name:       fmt.Sprintf("posix-%d", a.ID),
			ArchiveDir: a.Root,
			Checksums:  csc.ToPosix(),
		})
		if err != nil {
			return nil, fmt.Errorf("Unable to create new POSIX mover: %s", err)
		}

		movers[uint32(a.ID)] = mover
	}

	return movers, nil
}

func start(plugin dmplugin.Plugin, cfg *posixConfig) {
	// All base filesystem operations will be relative to current directory
	err := os.Chdir(plugin.Base())
	if err != nil {
		alert.Fatal(err)
	}

	done := make(chan struct{})
	interruptHandler(func() {
		close(done)
	})

	movers, err := createMovers(cfg)
	if err != nil {
		alert.Fatal(err)
	}

	for id, mover := range movers {
		plugin.AddMover(&dmplugin.Config{
			Mover:      mover,
			NumThreads: cfg.NumThreads,
			ArchiveID:  id,
		})
	}

	<-done
	plugin.Stop()
}

func getMergedConfig(plugin dmplugin.Plugin) (*posixConfig, error) {
	baseCfg := &posixConfig{
		Checksums: &checksumConfig{},
	}

	var cfg posixConfig
	err := dmplugin.LoadConfig(plugin.ConfigFile(), &cfg)
	if err != nil {
		return nil, fmt.Errorf("Failed to load config: %s", err)
	}

	return baseCfg.Merge(&cfg), nil
}

func main() {
	plugin, err := dmplugin.New(path.Base(os.Args[0]))
	if err != nil {
		alert.Fatalf("failed to initialize plugin: %s", err)
	}
	defer plugin.Close()

	cfg, err := getMergedConfig(plugin)
	if err != nil {
		alert.Fatalf("Unable to determine plugin configuration: %s", err)
	}

	debug.Printf("PosixMover configuration:\n%v", cfg)

	if len(cfg.Archives) == 0 {
		alert.Fatalf("Invalid configuration: No archives defined")
	}

	for _, archive := range cfg.Archives {
		debug.Print(archive)
		if err := archive.checkValid(); err != nil {
			alert.Fatalf("Invalid configuration: %s", err)
		}
	}

	start(plugin, cfg)
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
