package mntent

import (
	"github.com/pkg/errors"
	"github.intel.com/hpdd/lustre"
)

// GetMounted returns a slide of filesystem entries from
// the mounted fs table.
func GetMounted() (Entries, error) {
	return nil, errors.Wrap(lustre.ErrUnimplemented, "GetMounted")
}
