package agent

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/svclog"
)

type (
	rootDirFlag string
	archiveFlag string

	archiveMap map[ArchiveID]*PluginConfig

	// ArchiveID is a Lustre HSM archive number
	ArchiveID uint32

	// Config represents HSM Agent configuration
	Config struct {
		Lustre      fs.RootDir
		Processes   int
		Archives    archiveMap
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
	optArchives   archiveFlag
)

func init() {
	defaultConfig = newConfig()

	flag.StringVar(&optConfigPath, "config", DefaultConfigPath, "Path to agent config")
	flag.Var(&optRootDir, "root", "Lustre client mountpoint")
	flag.Var(&optArchives, "archive", "Archive definition(s) (number:plugin_bin:plugin_args)")
}

func (id ArchiveID) String() string {
	return strconv.FormatUint(uint64(id), 10)
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

func (m archiveMap) MarshalJSON() ([]byte, error) {
	strMap := make(map[string]*PluginConfig)

	for key, val := range m {
		strMap[strconv.Itoa(int(key))] = val
	}

	return json.Marshal(strMap)
}

func (m archiveMap) UnmarshalJSON(data []byte) error {
	strMap := make(map[string]*PluginConfig)

	if err := json.Unmarshal(data, strMap); err != nil {
		return err
	}

	for key, val := range strMap {
		num, err := strconv.ParseUint(key, 10, 32)
		if err != nil {
			return err
		}
		m[ArchiveID(num)] = val
	}

	return nil
}

func newConfig() *Config {
	return &Config{
		RPCPort:   4242,
		Processes: runtime.NumCPU(),
		Archives:  make(archiveMap),
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

func (f *archiveFlag) String() string {
	return string(*f)
}

func (f *archiveFlag) Set(value string) error {
	// number:plugin_bin:plugin_args
	fields := strings.Split(value, ":")
	if len(fields) != 3 {
		return fmt.Errorf("Unable to parse archive %q", value)
	}

	num, err := strconv.ParseUint(fields[0], 10, 32)
	if err != nil {
		return fmt.Errorf("Unable to parse archive %q: %s", value, err)
	}
	id := ArchiveID(num)

	defaultConfig.Archives[id] = &PluginConfig{
		BinPath: fields[1],
		Args:    strings.Fields(fields[2]),
	}
	svclog.Debug("Added %s as %d", defaultConfig.Archives[id], id)

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

	err := LoadConfig(optConfigPath, defaultConfig)
	if err != nil {
		if !(optConfigPath == DefaultConfigPath && os.IsNotExist(err)) {
			svclog.Fail("Failed to load config: %s", err)
		}
	}

	if !defaultConfig.Lustre.IsValid() {
		svclog.Fail("Invalid Lustre mountpoint %q", defaultConfig.Lustre)
	}

	if len(defaultConfig.Archives) == 0 {
		svclog.Fail("No archives configured")
	}

	return defaultConfig
}
