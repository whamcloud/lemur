package mntent

import (
	"os"

	"github.com/pkg/errors"
)

// GetMounted returns a slide of filesystem entries from
// the mounted fs table.
func GetMounted() (Entries, error) {
	fp, err := os.Open("/etc/mtab")
	if err != nil {
		return nil, errors.Wrap(err, "open failed")
	}
	return getEntries(fp)
}
