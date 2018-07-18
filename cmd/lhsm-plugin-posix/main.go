// Copyright (c) 2016 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package main

import (
	"os"
	"os/signal"
	"path"
	"syscall"

	"github.com/pkg/errors"

	"github.com/intel-hpdd/lemur/cmd/lhsm-plugin-posix/posix"
	"github.com/intel-hpdd/lemur/dmplugin"
	"github.com/intel-hpdd/lemur/pkg/fsroot"
	"github.com/intel-hpdd/logging/alert"
	"github.com/intel-hpdd/logging/debug"
)

type (
	posixConfig struct {
		NumThreads int                   `hcl:"num_threads"`
		Archives   posix.ArchiveSet      `hcl:"archive"`
		Checksums  *posix.ChecksumConfig `hcl:"checksums"`
	}
)

func (c *posixConfig) String() string {
	return dmplugin.DisplayConfig(c)
}

func (c *posixConfig) Merge(other *posixConfig) *posixConfig {
	result := new(posixConfig)

	if other.NumThreads > 0 {
		result.NumThreads = other.NumThreads
	} else {
		result.NumThreads = c.NumThreads

	}

	result.Archives = c.Archives.Merge(other.Archives)
	result.Checksums = c.Checksums.Merge(other.Checksums)

	return result
}

func start(plugin *dmplugin.Plugin, cfg *posixConfig) {
	// All base filesystem operations will be relative to current directory
	err := os.Chdir(plugin.Base())
	if err != nil {
		alert.Abort(errors.Wrap(err, "chdir failed"))
	}

	interruptHandler(func() {
		plugin.Stop()
	})

	for _, a := range cfg.Archives {
		mover, err := posix.NewMover(a)
		if err != nil {
			alert.Abort(errors.Wrap(err, "Unable to create new POSIX mover"))
		}

		plugin.AddMover(&dmplugin.Config{
			Mover:      mover,
			NumThreads: cfg.NumThreads,
			ArchiveID:  uint32(a.ID),
		})
	}

	plugin.Run()
}

func getMergedConfig(plugin *dmplugin.Plugin) (*posixConfig, error) {
	baseCfg := &posixConfig{
		Checksums: &posix.ChecksumConfig{},
	}

	var cfg posixConfig
	err := dmplugin.LoadConfig(plugin.ConfigFile(), &cfg)
	if err != nil {
		return nil, errors.Errorf("Failed to load config: %s", err)
	}

	return baseCfg.Merge(&cfg), nil
}

func main() {
	plugin, err := dmplugin.New(path.Base(os.Args[0]), func(path string) (fsroot.Client, error) {
		return fsroot.New(path)
	})
	if err != nil {
		alert.Abort(errors.Wrap(err, "failed to initialize plugin"))
	}
	defer plugin.Close()

	cfg, err := getMergedConfig(plugin)
	if err != nil {
		alert.Abort(errors.Wrap(err, "Unable to determine plugin configuration"))
	}

	debug.Printf("PosixMover configuration:\n%v", cfg)

	if len(cfg.Archives) == 0 {
		alert.Abort(errors.New("Invalid configuration: No archives defined"))
	}

	for _, archive := range cfg.Archives {
		debug.Print(archive)
		if err := archive.CheckValid(); err != nil {
			alert.Abort(errors.Wrap(err, "Invalid configuration"))
		}
	}

	posix.DefaultChecksums = *cfg.Checksums

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
