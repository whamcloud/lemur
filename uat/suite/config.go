package suite

import "fmt"

// Config holds configuration for the test suite
type Config struct {
	LustrePath string `hcl:"lustre_path" json:"lustre_path"`

	kv map[string]string
}

// Get attempts to get the value associated with key, or fails
func (c *Config) Get(key string) (string, error) {
	val, ok := c.kv[key]
	if !ok {
		return "", fmt.Errorf("No value for key %s found", key)
	}

	return val, nil
}

// Set inserts or updates a value for the given key
func (c *Config) Set(key, value string) {
	c.kv[key] = value
}

// NewConfig initializes a new Config instance with default values
func NewConfig() *Config {
	return &Config{
		kv: make(map[string]string),
	}
}

// LoadConfig attempts to load a config from the default location
func LoadConfig() (*Config, error) {
	return NewConfig(), nil
}
