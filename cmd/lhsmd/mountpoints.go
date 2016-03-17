package main

import (
	"os"
	"strings"

	"golang.org/x/sys/unix"

	"github.intel.com/hpdd/ce-tools/resources/mount"
	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/policy/pdm/lhsmd/agent"
)

func mountClient(dev, path string, cfgMountOptions []string) error {
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}

	// this is what mount_lustre.c does...
	mountOptions := append(cfgMountOptions, "device="+dev)

	var flags uintptr
	// LU-1783 -- force strictatime until a kernel vfs bug is fixed
	flags |= unix.MS_STRICTATIME
	return unix.Mount(dev, path, "lustre", flags, strings.Join(mountOptions, ","))
}

func getConfiguredMounts(cfg *agent.Config) []string {
	mountPoints := []string{cfg.AgentMountpoint}

	for _, plugin := range cfg.Plugins() {
		mountPoints = append(mountPoints, plugin.ClientMount)
	}

	return mountPoints
}

func configureMounts(cfg *agent.Config) error {
	fstab, err := mount.SystemMounts()
	if err != nil {
		return err
	}

	for _, mp := range getConfiguredMounts(cfg) {
		if fstab.FindByMount(mp) != nil {
			continue
		}
		debug.Printf("Mounting client at %s", mp)
		if err := mountClient(cfg.ClientDevice.String(), mp, cfg.ClientMountOptions); err != nil {
			return err
		}
	}

	return nil
}

// TODO: Make the agent hang around long enough to run this on shutdown,
// after deregistering with the coordinator and killing the movers.
func cleanupMounts(cfg *agent.Config) error {
	fstab, err := mount.SystemMounts()
	if err != nil {
		return err
	}

	for _, mp := range getConfiguredMounts(cfg) {
		if fstab.FindByMount(mp) == nil {
			continue
		}
		debug.Printf("Cleaning up %s", mp)
		if err := unix.Unmount(mp, 0); err != nil {
			alert.Warnf("Error while cleaning up %s: %s", mp, err)
			return err
		}
	}

	return nil
}
