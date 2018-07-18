// Copyright (c) 2016 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package harness

import (
	"fmt"
)

// HsmDriver defines an interface to be implemented by a HSM tool driver
// e.g. lfs hsm_*, ldmc, etc.
type HsmDriver interface {
	// Archive store's the file's data in the archive backend
	Archive(string) error
	// Restore explicitly restores the file
	Restore(string) error
	// Remove removes the restored file from the archive backend
	Remove(string) error
	// Release releases the archived file's space on the filesystem
	Release(string) error
	// GetState returns the HsmState for the file
	GetState(string) (HsmState, error)
}

// HsmState indicates the file's status
type HsmState string

const (
	// HsmUnknown indicates that the file state is unknown
	HsmUnknown HsmState = "unknown"

	// HsmUnmanaged indicates that the file is not managed by HSM
	HsmUnmanaged HsmState = "unmanaged"

	// HsmUnarchived indicates that the file is managed but unarchived
	HsmUnarchived HsmState = "unarchived"

	// HsmArchived indicates that the file is archived
	HsmArchived HsmState = "archived"

	// HsmReleased indicates that the file is archived and released
	HsmReleased HsmState = "released"
)

func (h HsmState) String() string {
	return string(h)
}

func getHsmDriver(cfg *Config) (HsmDriver, error) {
	if cfg.HsmDriver == "" {
		return NewMultiHsmDriver(), nil
	}

	fn, ok := HsmDrivers[cfg.HsmDriver]
	if !ok {
		return nil, fmt.Errorf("No HSM driver for %s found", cfg.HsmDriver)
	}

	return fn(), nil
}
