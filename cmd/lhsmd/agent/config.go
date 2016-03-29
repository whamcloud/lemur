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

	"github.com/hashicorp/hcl"
	"github.com/hashicorp/hcl/hcl/ast"

	"github.intel.com/hpdd/ce-tools/resources/lustre/clientmount"
	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/policy/pdm/lhsmd/config"
)

var (
	optConfigPath string
)

type (
	transportMap    map[string]*transportConfig
	transportConfig struct {
		Server string
		Port   int
	}

	// Config represents HSM Agent configuration
	Config struct {
		MountRoot          string `hcl:"mount_root"`
		AgentMountpoint    string `hcl:"agent_mountpoint"`
		ClientDevice       *clientmount.ClientDevice
		ClientMountOptions []string `hcl:"client_mount_options"`

		Processes int `hcl:"handler_count"`

		EnabledPlugins []string `hcl:"enabled_plugins"`
		PluginDir      string   `hcl:"plugin_dir"`

		Transports transportMap `hcl:"transport"`
	}
)

func (c *transportConfig) ConnectionString() string {
	if c.Port == 0 {
		return c.Server
	}
	return fmt.Sprintf("%s:%d", c.Server, c.Port)
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
		alert.Fatal(err)
	}

	var out bytes.Buffer
	json.Indent(&out, data, "", "\t")
	return out.String()
}

// Plugins returns a slice of *PluginConfig instances for enabled plugins
func (c *Config) Plugins() []*PluginConfig {
	var plugins []*PluginConfig

	// TODO: Decide if this really needs to be configurable, in which case
	// we need to make this better, or else just rip out everything other
	// than grpc.
	connectAt := c.Transports["grpc"].ConnectionString()
	for _, pluginName := range c.EnabledPlugins {
		binPath := path.Join(c.PluginDir, pluginName)
		plugins = append(plugins, NewPlugin(pluginName, binPath, connectAt, c.MountRoot))
	}

	return plugins
}

func defaultConfig() *Config {
	var cfgStr = `
mount_root = "/mnt/lhsmd"
agent_mountpoint = "/mnt/lhsmd/agent"
client_mount_options = ["user_xattr"]
plugin_dir = "/usr/share/lhsmd/plugins"

transport "grpc" {
	port = 4242
}
`
	cfg := &Config{
		Processes: runtime.NumCPU(),
	}

	if err := hcl.Decode(cfg, cfgStr); err != nil {
		alert.Fatalf("Error while generating default config: %s", err)
	}

	return cfg
}

// LoadConfig reads a config at the supplied path
func LoadConfig(configPath string, cfg *Config) error {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return err
	}

	obj, err := hcl.Parse(string(data))
	if err != nil {
		return err
	}

	if err := hcl.DecodeObject(cfg, obj); err != nil {
		return err
	}

	list, ok := obj.Node.(*ast.ObjectList)
	if !ok {
		return fmt.Errorf("Malformed config file")
	}

	f := list.Filter("client_device")
	if len(f.Items) == 0 {
		return fmt.Errorf("No client_device specified")
	}
	if len(f.Items) > 1 {
		return fmt.Errorf("Line %d: More than 1 client_device specified", f.Items[1].Assign.Line)
	}

	var devStr string
	if err := hcl.DecodeObject(&devStr, f.Elem().Items[0].Val); err != nil {
		return err
	}
	cfg.ClientDevice, err = clientmount.ClientDeviceFromString(devStr)
	if err != nil {
		return fmt.Errorf("Line %d: Invalid client_device %q: %s", f.Items[0].Assign.Line, devStr, err)
	}

	return err
}

// ConfigInitMust returns a valid *Config or fails trying
func ConfigInitMust() *Config {
	flag.Parse()

	cfg := defaultConfig()
	debug.Printf("loading config from %s", optConfigPath)
	err := LoadConfig(optConfigPath, cfg)
	if err != nil {
		if !(optConfigPath == config.DefaultConfigPath && os.IsNotExist(err)) {
			alert.Fatalf("Failed to load config: %s", err)
		}
	}
	if len(cfg.Transports) == 0 {
		alert.Fatal("Invalid configuration: No transports configured")
	}

	if _, err := os.Stat(cfg.PluginDir); os.IsNotExist(err) {
		alert.Fatalf("Invalid configuration: plugin_dir %q does not exist", cfg.PluginDir)
	}

	if len(cfg.EnabledPlugins) == 0 {
		alert.Fatal("Invalid configuration: No data mover plugins configured")
	}

	for _, plugin := range cfg.EnabledPlugins {
		pluginPath := path.Join(cfg.PluginDir, plugin)
		if _, err := os.Stat(pluginPath); os.IsNotExist(err) {
			alert.Fatalf("Invalid configuration: Plugin %q not found in %s", plugin, cfg.PluginDir)
		}
	}

	return cfg
}
