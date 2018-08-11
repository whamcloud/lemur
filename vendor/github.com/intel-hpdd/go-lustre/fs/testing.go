// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package fs

// TestID returns an ID value suitable for testing without an actual
// lustre filesystem.
func TestID(name string) ID {
	return ID(RootDir{path: name})
}
