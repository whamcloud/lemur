package agent

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"strings"

	"github.com/hashicorp/hcl"
	"github.com/hashicorp/hcl/hcl/ast"
	"github.com/pkg/errors"

	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/lustre/fs/spec"
	"github.intel.com/hpdd/policy/pdm/lhsmd/config"
)

var (
	optConfigPath string
)

type (
	transportConfig struct {
		Type   string `hcl:"type"`
		Server string `hcl:"server"`
		Port   int    `hcl:"port"`
	}

	influxConfig struct {
		URL      string `hcl:"url"`
		DB       string `hcl:"db"`
		User     string `hcl:"user"`
		Password string `hcl:"password"`
	}

	snapshotConfig struct {
		Enabled bool `hcl:"enabled"`
	}

	clientMountOptions []string

	// Config represents HSM Agent configuration
	Config struct {
		MountRoot          string             `hcl:"mount_root" json:"mount_root"`
		ClientDevice       *spec.ClientDevice `json:"client_device"`
		ClientMountOptions clientMountOptions `hcl:"client_mount_options" json:"client_mount_options"`

		Processes int `hcl:"handler_count" json:"handler_count"`

		InfluxDB *influxConfig `hcl:"influxdb" json:"influxdb"`

		EnabledPlugins []string `hcl:"enabled_plugins" json:"enabled_plugins"`
		PluginDir      string   `hcl:"plugin_dir" json:"plugin_dir"`

		Snapshots *snapshotConfig  `hcl:"snapshots" json:"snapshots"`
		Transport *transportConfig `hcl:"transport" json:"transport"`
	}
)

func (cmo clientMountOptions) HasOption(o string) bool {
	for _, option := range cmo {
		if option == o {
			return true
		}
	}
	return false
}

func (cmo clientMountOptions) String() string {
	return strings.Join(cmo, ",")
}

func (c *transportConfig) Merge(other *transportConfig) *transportConfig {
	result := new(transportConfig)

	result.Type = c.Type
	if other.Type != "" {
		result.Type = other.Type
	}

	result.Port = c.Port
	if other.Port > 0 {
		result.Port = other.Port
	}

	result.Server = c.Server
	if other.Server != "" {
		result.Server = other.Server
	}

	return result
}

func (c *transportConfig) ConnectionString() string {
	if c.Port == 0 {
		return c.Server
	}
	return fmt.Sprintf("%s:%d", c.Server, c.Port)
}

func (c *influxConfig) Merge(other *influxConfig) *influxConfig {
	result := new(influxConfig)

	result.URL = c.URL
	if other.URL != "" {
		result.URL = other.URL
	}

	result.DB = c.DB
	if other.DB != "" {
		result.DB = other.DB
	}

	result.User = c.User
	if other.User != "" {
		result.User = other.User
	}

	result.Password = c.Password
	if other.Password != "" {
		result.Password = other.Password
	}

	return result
}

func (c *snapshotConfig) Merge(other *snapshotConfig) *snapshotConfig {
	result := new(snapshotConfig)

	result.Enabled = other.Enabled

	return result
}

func init() {
	flag.StringVar(&optConfigPath, "config", config.DefaultConfigPath, "Path to agent config")

	// The CLI argument takes precedence, if both are set.
	if optConfigPath == config.DefaultConfigPath {
		if cfgDir := os.Getenv(config.ConfigDirEnvVar); cfgDir != "" {
			optConfigPath = path.Join(cfgDir, config.AgentConfigFile)
		}
	}

	// Ensure that it's set in our env so that plugins can use it to
	// find their own configs
	os.Setenv(config.ConfigDirEnvVar, path.Dir(optConfigPath))
}

func (c *Config) String() string {
	data, err := json.Marshal(c)
	if err != nil {
		alert.Abort(errors.Wrap(err, "marshal failed"))
	}

	var out bytes.Buffer
	json.Indent(&out, data, "", "\t")
	return out.String()
}

// Plugins returns a slice of *PluginConfig instances for enabled plugins
func (c *Config) Plugins() []*PluginConfig {
	var plugins []*PluginConfig

	connectAt := c.Transport.ConnectionString()
	for _, name := range c.EnabledPlugins {
		binPath := path.Join(c.PluginDir, name)
		plugin := NewPlugin(name, binPath, connectAt, c.MountRoot)
		plugins = append(plugins, plugin)
	}

	return plugins
}

// AgentMountpoint returns the calculated agent mountpoint under the
// agent mount root.
func (c *Config) AgentMountpoint() string {
	return path.Join(c.MountRoot, "agent")
}

// Merge combines the supplied configuration's values with this one's
func (c *Config) Merge(other *Config) *Config {
	result := new(Config)

	result.MountRoot = c.MountRoot
	if other.MountRoot != "" {
		result.MountRoot = other.MountRoot
	}

	result.ClientDevice = c.ClientDevice
	if other.ClientDevice != nil {
		result.ClientDevice = other.ClientDevice
	}

	result.ClientMountOptions = c.ClientMountOptions
	for _, otherOption := range other.ClientMountOptions {
		if result.ClientMountOptions.HasOption(otherOption) {
			continue
		}
		result.ClientMountOptions = append(result.ClientMountOptions, otherOption)
	}

	result.Processes = c.Processes
	if other.Processes > result.Processes {
		result.Processes = other.Processes
	}

	result.InfluxDB = c.InfluxDB
	if other.InfluxDB != nil {
		result.InfluxDB = result.InfluxDB.Merge(other.InfluxDB)
	}

	result.EnabledPlugins = c.EnabledPlugins
	if len(other.EnabledPlugins) > 0 {
		result.EnabledPlugins = other.EnabledPlugins
	}

	result.PluginDir = c.PluginDir
	if other.PluginDir != "" {
		result.PluginDir = other.PluginDir
	}

	result.Snapshots = c.Snapshots
	if other.Snapshots != nil {
		result.Snapshots = result.Snapshots.Merge(other.Snapshots)
	}

	result.Transport = c.Transport
	if other.Transport != nil {
		result.Transport = result.Transport.Merge(other.Transport)
	}

	return result
}

// DefaultConfig initializes a new Config struct with default values
func DefaultConfig() *Config {
	cfg := NewConfig()
	cfg.MountRoot = config.DefaultAgentMountRoot
	cfg.ClientMountOptions = config.DefaultClientMountOptions
	cfg.PluginDir = config.DefaultPluginDir
	cfg.Processes = runtime.NumCPU()
	cfg.Transport = &transportConfig{
		Type: config.DefaultTransport,
		Port: config.DefaultTransportPort,
	}
	return cfg
}

// NewConfig initializes a new Config struct with zero values
func NewConfig() *Config {
	return &Config{
		InfluxDB:           &influxConfig{},
		Snapshots:          &snapshotConfig{},
		Transport:          &transportConfig{},
		EnabledPlugins:     []string{},
		ClientMountOptions: clientMountOptions{},
	}
}

// LoadConfig reads a config at the supplied path
func LoadConfig(configPath string) (*Config, error) {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, errors.Wrap(err, "read failed")
	}

	obj, err := hcl.Parse(string(data))
	if err != nil {
		return nil, errors.Wrap(err, "parse config failed")
	}

	defaults := DefaultConfig()
	cfg := NewConfig()
	if err := hcl.DecodeObject(cfg, obj); err != nil {
		return nil, errors.Wrap(err, "decode config failed")
	}
	cfg = defaults.Merge(cfg)

	list, ok := obj.Node.(*ast.ObjectList)
	if !ok {
		return nil, errors.Errorf("Malformed config file")
	}

	f := list.Filter("client_device")
	if len(f.Items) == 0 {
		return nil, errors.Errorf("No client_device specified")
	}
	if len(f.Items) > 1 {
		return nil, errors.Errorf("Line %d: More than 1 client_device specified", f.Items[1].Assign.Line)
	}

	var devStr string
	if err := hcl.DecodeObject(&devStr, f.Elem().Items[0].Val); err != nil {
		return nil, errors.Wrap(err, "decode device failed")
	}
	cfg.ClientDevice, err = spec.ClientDeviceFromString(devStr)
	if err != nil {
		return nil, errors.Wrapf(err, "Line %d: Invalid client_device %q", f.Items[0].Assign.Line, devStr)
	}

	return cfg, nil
}

// ConfigInitMust returns a valid *Config or fails trying
func ConfigInitMust() *Config {
	flag.Parse()

	debug.Printf("loading config from %s", optConfigPath)
	cfg, err := LoadConfig(optConfigPath)
	if err != nil {
		if !(optConfigPath == config.DefaultConfigPath && os.IsNotExist(err)) {
			alert.Abort(errors.Wrap(err, "Failed to load config"))
		}
	}

	if cfg.Transport == nil {
		alert.Abort(errors.New("Invalid configuration: No transports configured"))
	}

	if _, err := os.Stat(cfg.PluginDir); os.IsNotExist(err) {
		alert.Abort(errors.Errorf("Invalid configuration: plugin_dir %q does not exist", cfg.PluginDir))
	}

	if len(cfg.EnabledPlugins) == 0 {
		alert.Abort(errors.New("Invalid configuration: No data mover plugins configured"))
	}

	for _, plugin := range cfg.EnabledPlugins {
		pluginPath := path.Join(cfg.PluginDir, plugin)
		if _, err := os.Stat(pluginPath); os.IsNotExist(err) {
			alert.Abort(errors.Errorf("Invalid configuration: Plugin %q not found in %s", plugin, cfg.PluginDir))
		}
	}

	return cfg
}
