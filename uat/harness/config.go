package harness

// Config holds configuration for the test harness
type Config struct {
	HsmDriver        string `hcl:"hsm_driver" json:"hsm_driver"`
	LustrePath       string `hcl:"lustre_path" json:"lustre_path"`
	CleanupOnFailure bool   `hcl:"cleanup_on_failure" json:"cleanup_on_failure"`
	EnableAgentDebug bool   `hcl:"enable_agent_debug" json:"enable_agent_debug"`
}

// NewConfig initializes a new Config instance with default values
func NewConfig() *Config {
	return &Config{}
}

// LoadConfig attempts to load a config from the default location
func LoadConfig() (*Config, error) {
	return NewConfig(), nil
}
