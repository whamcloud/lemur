package agent

import (
	"fmt"
	"os/exec"
	"path"

	"github.intel.com/hpdd/svclog"
)

type (
	// PluginConfig represents configuration for a single plugin
	PluginConfig struct {
		BinPath string
		Args    []string
	}
)

func (c *PluginConfig) String() string {
	return fmt.Sprintf("%s: %s", c.BinPath, c.Args)
}

func startPlugin(id ArchiveID, cfg *PluginConfig) (*exec.Cmd, error) {
	svclog.Debug("Starting %s for %d", cfg.BinPath, id)

	cmd := exec.Command(cfg.BinPath, cfg.Args...)

	prefix := path.Base(cfg.BinPath)
	cmd.Stdout = svclog.Writer().Prefix(prefix)
	cmd.Stderr = svclog.Writer().Prefix(prefix + "-stderr")

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	return cmd, nil
}
