// Copyright (c) 2016 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package hsm

import (
	"os"

	"github.com/intel-hpdd/go-lustre"
	"github.com/intel-hpdd/go-lustre/llapi"
)

// Import file as a released file.
func Import(f string, archive uint, fi os.FileInfo, layout *llapi.DataLayout) (*lustre.Fid, error) {
	return llapi.HsmImport(f, archive, fi, layout)
}
