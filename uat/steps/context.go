// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package steps

import "github.intel.com/hpdd/lemur/uat/harness"

// Package-level singleton which provides per-scenario context. Super
// unhappy about this design, but the alternatives were more awkward.
var ctx *harness.ScenarioContext

// RegisterContext resets the package-level singleton
func RegisterContext(newCtx *harness.ScenarioContext) {
	ctx = newCtx
}
