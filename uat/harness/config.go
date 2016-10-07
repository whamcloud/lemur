// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package harness

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"os/user"
	"path"

	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/debug"

	"github.com/hashicorp/hcl"
	"github.com/pkg/errors"
)

const (
	// UATConfigFile is the name of the harness configuration file
	UATConfigFile = ".lhsmd-test"

	// UATConfigEnvVar is the name of the optional environment variable that
	// may be set to specify config location
	UATConfigEnvVar = "LHSMD_UAT_CONFIG_FILE"
)

// Config holds configuration for the test harness
type Config struct {
	HsmDriver        string `hcl:"hsm_driver" json:"hsm_driver"`
	LustrePath       string `hcl:"lustre_path" json:"lustre_path"`
	CleanupOnFailure bool   `hcl:"cleanup_on_failure" json:"cleanup_on_failure"`
	EnableAgentDebug bool   `hcl:"enable_agent_debug" json:"enable_agent_debug"`

	AWSAccessKeyID     string `hcl:"aws_access_key_id" json:"aws_access_key_id"`
	AWSSecretAccessKey string `hcl:"aws_secret_access_key" json:"aws_secret_access_key"`
	S3Region           string `hcl:"s3_region" json:"s3_region"`
	S3Endpoint         string `hcl:"s3_endpoint" json:"s3_endpoint"`
	S3Bucket           string `hcl:"s3_bucket" json:"s3_bucket"`
	S3Prefix           string `hcl:"s3_prefix" json:"s3_prefix"`
}

// Merge combines this config's values with the other config's values
func (c *Config) Merge(other *Config) *Config {
	result := new(Config)

	result.HsmDriver = c.HsmDriver
	if other.HsmDriver != "" {
		result.HsmDriver = other.HsmDriver
	}

	result.LustrePath = c.LustrePath
	if other.LustrePath != "" {
		result.LustrePath = other.LustrePath
	}

	result.CleanupOnFailure = other.CleanupOnFailure
	result.EnableAgentDebug = other.EnableAgentDebug

	result.S3Region = c.S3Region
	if other.S3Region != "" {
		result.S3Region = other.S3Region
	}

	result.S3Endpoint = c.S3Endpoint
	if other.S3Endpoint != "" {
		result.S3Endpoint = other.S3Endpoint
	}

	result.S3Bucket = c.S3Bucket
	if other.S3Bucket != "" {
		result.S3Bucket = other.S3Bucket
	}

	result.S3Prefix = c.S3Prefix
	if other.S3Prefix != "" {
		result.S3Prefix = other.S3Prefix
	}

	result.AWSAccessKeyID = c.AWSAccessKeyID
	if other.AWSAccessKeyID != "" {
		result.AWSAccessKeyID = other.AWSAccessKeyID
	}

	result.AWSSecretAccessKey = c.AWSSecretAccessKey
	if other.AWSSecretAccessKey != "" {
		result.AWSSecretAccessKey = other.AWSSecretAccessKey
	}

	return result
}

func (c *Config) String() string {
	data, err := json.Marshal(c)
	if err != nil {
		alert.Abort(errors.Wrap(err, "couldn't marshal test config to json"))
	}

	var out bytes.Buffer
	json.Indent(&out, data, "", "\t")
	return out.String()
}

// NewConfig initializes a new Config instance with default values
func NewConfig() *Config {
	return &Config{
		S3Region: "us-east-1",
	}
}

// LoadConfig attempts to load a config from one of the default locations
func LoadConfig() (*Config, error) {
	cfg := NewConfig()

	user, err := user.Current()
	if err != nil {
		return nil, errors.Wrap(err, "Unable to get current user")
	}
	cwd, err := os.Getwd()
	if err != nil {
		return nil, errors.Wrap(err, "Unable to get current directory")
	}
	// list of locations to try, in decreasing precedence
	locations := []string{
		os.Getenv(UATConfigEnvVar),
		path.Join(cwd, UATConfigFile),
		path.Join(user.HomeDir, UATConfigFile),
	}

	for _, location := range locations {
		debug.Printf("trying to load config from %s", location)
		if loaded, err := loadConfigFile(location); err == nil {
			cfg = cfg.Merge(loaded)
			break
		}
	}

	debug.Printf("Harness config: %s", cfg)
	return cfg, nil
}

func loadConfigFile(cfgPath string) (*Config, error) {
	cfg := NewConfig()

	data, err := ioutil.ReadFile(cfgPath)
	if err != nil {
		return nil, err
	}

	if err := hcl.Decode(cfg, string(data)); err != nil {
		alert.Warnf("config file error %s:%s", cfgPath, err)
		return nil, err
	}

	return cfg, nil
}
