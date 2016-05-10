package agent

import "github.intel.com/hpdd/lustre/hsm"

// TestAgent returns an *HsmAgent configured for testing
func TestAgent(cfg *Config, mon *PluginMonitor, as hsm.ActionSource, ep *Endpoints) *HsmAgent {
	return &HsmAgent{
		stats:        NewActionStats(),
		config:       cfg,
		monitor:      mon,
		actionSource: as,
		Endpoints:    ep,
	}
}
