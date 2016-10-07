// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package mntent

import "bytes"

// TestEntries returns a new Entries slice based on the supplied string
func TestEntries(raw string) (Entries, error) {
	return getEntries(bytes.NewBufferString(raw))
}
