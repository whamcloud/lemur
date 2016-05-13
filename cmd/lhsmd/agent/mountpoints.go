package agent

import (
	"fmt"
	"os"

	"github.com/pkg/errors"

	"golang.org/x/sys/unix"

	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/lustre/pkg/mntent"
)

type (
	mountConfig struct {
		Device    string
		Directory string
		Type      string
		Options   clientMountOptions
		Flags     uintptr
	}
)

func (mc *mountConfig) String() string {
	return fmt.Sprintf("%s %s %s %s (%d)", mc.Device, mc.Directory, mc.Type, mc.Options, mc.Flags)
}

func mountClient(cfg *mountConfig) error {
	if err := os.MkdirAll(cfg.Directory, 0700); err != nil {
		return errors.Wrap(err, "mkdir failed")
	}

	return unix.Mount(cfg.Device, cfg.Directory, cfg.Type, cfg.Flags, cfg.Options.String())
}

func createMountConfigs(cfg *Config) []*mountConfig {
	device := cfg.ClientDevice.String()
	// this is what mount_lustre.c does...
	opts := append(cfg.ClientMountOptions, "device="+device)

	var flags uintptr
	// LU-1783 -- force strictatime until a kernel vfs bug is fixed
	flags |= unix.MS_STRICTATIME

	// Create the agent mountpoint first, then add per-plugin mountpoints
	configs := []*mountConfig{
		&mountConfig{
			Device:    device,
			Directory: cfg.AgentMountpoint,
			Type:      "lustre",
			Options:   opts,
			Flags:     flags,
		},
	}

	for _, plugin := range cfg.Plugins() {
		configs = append(configs, &mountConfig{
			Device:    device,
			Directory: plugin.ClientMount,
			Type:      "lustre",
			Options:   opts,
			Flags:     flags,
		})
	}

	return configs
}

// ConfigureMounts configures a set of Lustre client mounts; one for the agent
// and one for each configure data mover.
func ConfigureMounts(cfg *Config) error {
	entries, err := mntent.GetMounted()
	if err != nil {
		return errors.Wrap(err, "failed to get list of mounted filesystems")
	}

	for _, mc := range createMountConfigs(cfg) {
		if _, err := entries.ByDir(mc.Directory); err == nil {
			continue
		}

		debug.Printf("Mounting client at %s", mc.Directory)
		if err := mountClient(mc); err != nil {
			return errors.Wrap(err, "mount client failed")
		}
	}

	return nil
}

// CleanupMounts unmounts the Lustre client mounts configured by
// ConfigureMounts().
func CleanupMounts(cfg *Config) error {
	entries, err := mntent.GetMounted()
	if err != nil {
		return errors.Wrap(err, "failed to get list of mounted filesystems")
	}

	for _, mc := range createMountConfigs(cfg) {
		if _, err := entries.ByDir(mc.Directory); err != nil {
			continue
		}

		debug.Printf("Cleaning up %s", mc.Directory)
		if err := unix.Unmount(mc.Directory, 0); err != nil {
			// Non-fatal error; just log it.
			alert.Warnf("Error while cleaning up Lustre mountpoints: %s", err)
		}
	}

	return nil
}
