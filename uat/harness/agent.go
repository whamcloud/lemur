package harness

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"syscall"

	"github.com/pkg/errors"

	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/lustre/fs/spec"
	"github.intel.com/hpdd/lustre/pkg/mntent"
	"github.intel.com/hpdd/policy/pdm/lhsmd/agent"
	defaults "github.intel.com/hpdd/policy/pdm/lhsmd/config"
)

// AgentDriver allows the harness to drive an HSM agent
type AgentDriver struct {
	ac      *agent.Config
	cmd     *exec.Cmd
	started bool
}

const (
	// HsmAgentCfgKey refers to this context's agent config file
	HsmAgentCfgKey = "agent_config_key"

	// HsmAgentBinary is the name of the HSM Agent
	HsmAgentBinary = "lhsmd"

	// HsmPluginPrefix is the base name of data mover plugins
	HsmPluginPrefix = "lhsm-plugin-"
)

// ConfigureAgent creates or updates the Context's agent config
func ConfigureAgent(ctx *ScenarioContext) error {
	cd, err := getClientDeviceForMount(ctx.Config.LustrePath)
	if err != nil {
		return errors.Wrapf(err, "Failed to get client device for %s", ctx.Config.LustrePath)
	}

	agentPath, err := exec.LookPath(HsmAgentBinary)
	if err != nil {
		return errors.Wrap(err, "Unable to determine plugin dir based on lhsmd location")
	}

	agentConfig := agent.NewConfig()
	agentConfig.PluginDir = path.Dir(agentPath)
	agentConfig.MountRoot = ctx.Workdir() + "/mnt"
	agentConfig.ClientDevice = cd

	// Maybe this should be an error?
	if ctx.AgentDriver != nil {
		alert.Warn("Updating existing agent driver in context")
	}

	cfgFile := ctx.Workdir() + defaults.DefaultConfigPath
	ctx.SetKey(HsmAgentCfgKey, cfgFile)

	ctx.AgentDriver, err = newAgentDriver(ctx, agentConfig)
	if err != nil {
		return errors.Wrap(err, "Unable to create agent driver")
	}

	return WriteAgentConfig(ctx)
}

// WriteAgentConfig writes the agent configuration into the workdir
func WriteAgentConfig(ctx *ScenarioContext) error {
	if ctx.AgentDriver == nil || ctx.AgentDriver.ac == nil {
		return fmt.Errorf("WriteAgentConfig() may only be called after ConfigureAgent()")
	}

	cfgFile, err := ctx.GetKey(HsmAgentCfgKey)
	if err != nil {
		return errors.Wrap(err, "No config file path found")
	}

	cfgDir := path.Dir(cfgFile)
	if err := os.MkdirAll(cfgDir, 0755); err != nil {
		return errors.Wrap(err, "Failed to create agent config dir")
	}
	return ioutil.WriteFile(cfgFile, []byte(ctx.AgentDriver.ac.String()), 0644)
}

// StartAgent starts the configured agent
func StartAgent(ctx *ScenarioContext) error {
	if ctx.AgentDriver == nil || ctx.AgentDriver.cmd == nil {
		return fmt.Errorf("StartAgent() may only be called after ConfigureAgent()")
	}

	ctx.AgentDriver.started = true
	return ctx.AgentDriver.cmd.Start()
}

func newAgentCmd(ctx *ScenarioContext) (*exec.Cmd, error) {
	cfgFile, err := ctx.GetKey(HsmAgentCfgKey)
	if err != nil {
		return nil, errors.Wrap(err, "No config file path found")
	}

	stdout, err := os.OpenFile(ctx.Workdir()+"/agent.stdout", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, errors.Wrap(err, "Can't create agent stdout file")
	}
	stderr, err := os.OpenFile(ctx.Workdir()+"/agent.stderr", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, errors.Wrap(err, "Can't create agent stderr file")
	}

	agentArgs := []string{"-config=" + cfgFile}
	if ctx.Config.EnableAgentDebug {
		agentArgs = append(agentArgs, "-debug")
	}
	cmd := exec.Command(HsmAgentBinary, agentArgs...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	return cmd, nil
}

func newAgentDriver(ctx *ScenarioContext, cfg *agent.Config) (*AgentDriver, error) {
	cmd, err := newAgentCmd(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to create agent cmd")
	}

	driver := &AgentDriver{
		ac:  cfg,
		cmd: cmd,
	}

	return driver, nil
}

// StopAgent stops the running agent
func StopAgent(ctx *ScenarioContext) error {
	if ctx.AgentDriver == nil || ctx.AgentDriver.cmd == nil {
		return fmt.Errorf("StopAgent() may only be called after StartAgent()")
	}
	if !ctx.AgentDriver.started {
		return nil
	}

	ad := ctx.AgentDriver
	if ad.cmd.ProcessState != nil && ad.cmd.ProcessState.Exited() {
		return fmt.Errorf("StopAgent() called on stopped agent")
	}

	// Send SIGTERM to allow the agent to clean up
	if err := ad.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return errors.Wrap(err, "sending SIGTERM to agent failed")
	}

	if err := ad.cmd.Wait(); err != nil {
		return errors.Wrapf(err, "agent did not exit cleanly")
	}

	return nil
}

func writePosixMoverConfig(ctx *ScenarioContext) error {
	cfg := fmt.Sprintf(`archive "one" {
id = 1
root = "%s"
}`, ctx.Workdir()+"/archives/1")

	cfgFile := ctx.Workdir() + "/etc/lhsmd/lhsm-plugin-posix"
	cfgDir := path.Dir(cfgFile)
	if err := os.MkdirAll(cfgDir, 0755); err != nil {
		return errors.Wrap(err, "Failed to create plugin config dir")
	}

	return ioutil.WriteFile(cfgFile, []byte(cfg), 0644)
}

func writeMoverConfig(ctx *ScenarioContext, name string) error {
	switch name {
	case "lhsm-plugin-posix":
		return writePosixMoverConfig(ctx)
	default:
		return fmt.Errorf("Unknown data mover in test: %s", name)
	}
}

// AddConfiguredMover adds a data mover to the agent configuration
func AddConfiguredMover(ctx *ScenarioContext, name string) error {
	if ctx.AgentDriver == nil || ctx.AgentDriver.ac == nil {
		return fmt.Errorf("AddConfiguredMover() may only be called after ConfigureAgent()")
	}

	ac := ctx.AgentDriver.ac
	ac.EnabledPlugins = append(ac.EnabledPlugins, name)

	if err := writeMoverConfig(ctx, name); err != nil {
		return errors.Wrap(err, "Failed to write data mover config")
	}

	// Write updated agent config
	return WriteAgentConfig(ctx)
}

func getClientDeviceForMount(mnt string) (*spec.ClientDevice, error) {
	entry, err := mntent.GetEntryByDir(mnt)
	if err != nil {
		return nil, err
	}

	return spec.ClientDeviceFromString(entry.Fsname)
}
