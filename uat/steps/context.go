package steps

import "github.com/intel-hpdd/lemur/uat/harness"

// Package-level singleton which provides per-scenario context. Super
// unhappy about this design, but the alternatives were more awkward.
var ctx *harness.ScenarioContext

// RegisterContext resets the package-level singleton
func RegisterContext(newCtx *harness.ScenarioContext) {
	ctx = newCtx
}
