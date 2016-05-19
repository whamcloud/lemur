package agent

import (
	"fmt"
	"os"
	"time"

	"github.com/pkg/errors"

	"golang.org/x/net/context"
	"golang.org/x/sys/unix"

	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/lustre/pkg/mntent"
)

// UnmountTimeout is the time, in seconds, that an unmount will be retried
// before failing with an error.
const UnmountTimeout = 10

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
			Directory: cfg.AgentMountpoint(),
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

func doTimedUnmount(dir string) error {
	done := make(chan struct{})
	lastError := make(chan error)

	// This feels a little baroque, but it accomplishes two goals:
	// 1) Don't leak this goroutine if we time out
	// 2) Make sure that we safely get an error from unix.Unmount
	//    back out to the caller
	ctx, cancel := context.WithCancel(context.Background())
	go func(ctx context.Context) {
		var err error
		for {
			select {
			case <-ctx.Done():
				lastError <- err
			default:
				err = unix.Unmount(dir, 0)
				if err == nil {
					close(done)
					lastError <- err
					return
				}
				time.Sleep(1 * time.Second)
			}
		}
	}(ctx)

	for {
		select {
		case <-done:
			return <-lastError
		case <-time.After(time.Duration(UnmountTimeout) * time.Second):
			cancel()
			return errors.Wrapf(<-lastError, "Unmount of %s timed out", dir)
		}
	}
}

// CleanupMounts unmounts the Lustre client mounts configured by
// ConfigureMounts().
func CleanupMounts(cfg *Config) error {
	entries, err := mntent.GetMounted()
	if err != nil {
		return errors.Wrap(err, "failed to get list of mounted filesystems")
	}

	// Reverse the generated slice to perform mover unmounts first,
	// finishing with the agent unmount.
	mcList := createMountConfigs(cfg)
	revList := make([]*mountConfig, len(mcList))
	for i := range mcList {
		revList[i] = mcList[len(mcList)-1-i]
	}

	for _, mc := range revList {
		if _, err := entries.ByDir(mc.Directory); err != nil {
			continue
		}

		debug.Printf("Cleaning up %s", mc.Directory)
		if err := doTimedUnmount(mc.Directory); err != nil {
			return errors.Wrapf(err, "Failed to unmount %s", mc.Directory)
		}
	}

	return nil
}
