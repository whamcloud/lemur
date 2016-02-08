package agent

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"strings"

	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/svclog"
)

type (
	rootDirFlag string
	pluginFlag  string

	pluginMap map[string]*PluginConfig

	// Config represents HSM Agent configuration
	Config struct {
		Lustre      fs.RootDir
		Processes   int
		Plugins     pluginMap
		RPCPort     int
		RedisServer string
	}
)

const (
	// DefaultConfigPath is the default path to the agent config file
	DefaultConfigPath = "/etc/lhsmd/config.json"
)

var (
	defaultConfig *Config

	optConfigPath string
	optRootDir    rootDirFlag
	optPlugins    pluginFlag
	optProcesses  int
)

func init() {
	defaultConfig = newConfig()

	flag.StringVar(&optConfigPath, "config", DefaultConfigPath, "Path to agent config")
	flag.Var(&optRootDir, "root", "Lustre client mountpoint")
	flag.Var(&optPlugins, "plugin", "Plugin definition(s) (name:plugin_bin:plugin_args)")
	flag.IntVar(&optProcesses, "n", runtime.NumCPU(), "Number of handler threads")
}

func (c *Config) String() string {
	data, err := json.Marshal(c)
	if err != nil {
		svclog.Fail(err)
	}

	var out bytes.Buffer
	json.Indent(&out, data, "", "\t")
	return out.String()
}

func newConfig() *Config {
	return &Config{
		RPCPort: 4242,
		Plugins: make(pluginMap),
	}
}

func (f *rootDirFlag) String() string {
	return string(*f)
}

func (f *rootDirFlag) Set(value string) error {
	root, err := fs.MountRoot(value)
	if err != nil {
		return err
	}
	defaultConfig.Lustre = root

	return nil
}

func (f *pluginFlag) String() string {
	return string(*f)
}

func (f *pluginFlag) Set(value string) error {
	// name:plugin_bin:plugin_args
	fields := strings.Split(value, ":")
	if len(fields) != 3 {
		return fmt.Errorf("Unable to parse archive %q", value)
	}

	name := fields[0]
	defaultConfig.Plugins[name] = NewPlugin(
		name, fields[1], strings.Fields(fields[2])...,
	)
	svclog.Debug("Added %s as %s", defaultConfig.Plugins[name], name)

	return nil
}

// LoadConfig reads a config at the supplied path
func LoadConfig(configPath string, cfg *Config) error {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return err
	}

	err = json.Unmarshal(data, cfg)
	if err != nil {
		return err
	}

	return nil
}

// ConfigInitMust returns a valid *Config or fails trying
func ConfigInitMust() *Config {
	flag.Parse()

	defaultConfig.Processes = optProcesses

	err := LoadConfig(optConfigPath, defaultConfig)
	if err != nil {
		if !(optConfigPath == DefaultConfigPath && os.IsNotExist(err)) {
			svclog.Fail("Failed to load config: %s", err)
		}
	}

	if !defaultConfig.Lustre.IsValid() {
		svclog.Fail("Invalid Lustre mountpoint %q", defaultConfig.Lustre)
	}

	if len(defaultConfig.Plugins) == 0 {
		svclog.Fail("No data mover plugins configured")
	}

	return defaultConfig
}
