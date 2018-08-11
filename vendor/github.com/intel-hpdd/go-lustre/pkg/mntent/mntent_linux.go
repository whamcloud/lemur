// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

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
