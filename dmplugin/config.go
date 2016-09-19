package dmplugin

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"

	"github.com/hashicorp/hcl"
	"github.com/pkg/errors"
	"github.intel.com/hpdd/lemur/cmd/lhsmd/config"
	"github.intel.com/hpdd/logging/alert"
)

type pluginConfig struct {
	AgentAddress string
	ClientRoot   string
	ConfigDir    string
}

// LoadConfig reads this plugin's config file and decodes it into the passed
// config struct.
func LoadConfig(cfgFile string, cfg interface{}) error {
	// Ensure config file is private
	fi, err := os.Stat(cfgFile)
	if err != nil {
		return errors.Wrap(err, "stat config file failed")
	}
	if (int(fi.Mode()) & 077) != 0 {
		return errors.New("config file permisisons are insecure")
	}

	data, err := ioutil.ReadFile(cfgFile)
	if err != nil {
		return errors.Wrap(err, "read config file failed")
	}

	if err := hcl.Decode(cfg, string(data)); err != nil {
		return errors.Wrap(err, "decode config file failed")
	}

	return nil
}

// DisplayConfig formats the configuration into string for display
// purposes.  A helper function to remove some duplicate code in
// movers.
func DisplayConfig(cfg interface{}) string {
	data, err := json.Marshal(cfg)
	if err != nil {
		alert.Abort(errors.Wrap(err, "marshal config failed"))
	}

	var out bytes.Buffer
	json.Indent(&out, data, "", "\t")
	return out.String()
}

func getAgentEnvSetting(name string) (value string) {
	if value = os.Getenv(name); value == "" {
		alert.Fatal("This plugin is intended to be launched by the agent.")
	}
	return
}

// mustInitConfig looks for the plugin environment variables and
// returns the configuratino. Will fail the process with hlpeful
// message if any of the env variables are not seet.
func mustInitConfig() *pluginConfig {
	pc := &pluginConfig{
		AgentAddress: getAgentEnvSetting(config.AgentConnEnvVar),
		ClientRoot:   getAgentEnvSetting(config.PluginMountpointEnvVar),
		ConfigDir:    getAgentEnvSetting(config.ConfigDirEnvVar),
	}
	return pc
}
