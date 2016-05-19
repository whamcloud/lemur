package drivers

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os/exec"
	"path"
	"syscall"

	"github.com/pkg/errors"

	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/lustre/fs/spec"
	"github.intel.com/hpdd/lustre/pkg/mntent"
	"github.intel.com/hpdd/policy/pdm/lhsmd/agent"
	"github.intel.com/hpdd/policy/pdm/uat/suite"
)

// AgentDriver allows the harness to drive an HSM agent
type AgentDriver struct {
	ac      *agent.Config
	sc      *suite.Config
	cmd     *exec.Cmd
	workdir string
	Out     bytes.Buffer
	Err     bytes.Buffer
}

// HsmAgentBinary is the name of the HSM Agent
const HsmAgentBinary = "lhsmd"

// StartAgent starts an agent
func (ad *AgentDriver) StartAgent(cfgPath string) error {
	debug.Printf("ad in StartAgent(): %v", ad)

	ad.cmd = exec.Command(HsmAgentBinary, "-config="+cfgPath, "-debug")
	ad.cmd.Stdout = &ad.Out
	ad.cmd.Stderr = &ad.Err

	if err := ad.cmd.Start(); err != nil {
		return errors.Wrapf(err, "failed to start agent, out: %s, err: %s", ad.Out.String(), ad.Err.String())
	}

	return nil
}

// StopAgent stops the running agent
func (ad *AgentDriver) StopAgent() error {
	debug.Printf("ad in StopAgent(): %v", ad)

	if ad.cmd == nil {
		return fmt.Errorf("StopAgent() called with nil cmd")
	}
	if ad.cmd.ProcessState != nil && ad.cmd.ProcessState.Exited() {
		return fmt.Errorf("StopAgent() called on stopped agent")
	}

	// Send SIGTERM to allow the agent to clean up
	if err := ad.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return errors.Wrap(err, "sending SIGTERM to agent failed")
	}

	if err := ad.cmd.Wait(); err != nil {
		return errors.Wrapf(err, "agent did not exit cleanly, out: %s, err: %s", ad.Out.String(), ad.Err.String())
	}

	return nil
}

func (ad *AgentDriver) writePosixMoverAgentDriver() error {
	cfg := fmt.Sprintf(`archive "one" {
id = 1
root = "%s"
}`, ad.workdir+"/archives/1")

	return ioutil.WriteFile(ad.workdir+"/etc/lhsmd/lhsm-plugin-posix", []byte(cfg), 0644)
}

func (ad *AgentDriver) writeMoverAgentDriver(name string) error {
	switch name {
	case "lhsm-plugin-posix":
		return ad.writePosixMoverAgentDriver()
	default:
		return fmt.Errorf("Unknown data mover in test: %s", name)
	}
}

// AddConfiguredMover adds a data mover to the agent configuration
func (ad *AgentDriver) AddConfiguredMover(name string) error {
	ad.ac.EnabledPlugins = append(ad.ac.EnabledPlugins, name)

	return ad.writeMoverAgentDriver(name)
}

func getClientDeviceForMount(mnt string) (*spec.ClientDevice, error) {
	entry, err := mntent.GetEntryByDir(mnt)
	if err != nil {
		return nil, err
	}

	return spec.ClientDeviceFromString(entry.Fsname)
}

// WriteAgentConfig writes out the HSM Agent configuration
func (ad *AgentDriver) WriteAgentConfig() (string, error) {
	cd, err := getClientDeviceForMount(ad.sc.LustrePath)
	if err != nil {
		return "", errors.Wrapf(err, "Failed to get client device for %s", ad.sc.LustrePath)
	}

	agentPath, err := exec.LookPath("lhsmd")
	if err != nil {
		return "", errors.Wrap(err, "Unable to determine plugin dir based on lhsmd location")
	}

	ad.ac.PluginDir = path.Dir(agentPath)
	ad.ac.MountRoot = ad.workdir + "/mnt"
	ad.ac.ClientDevice = cd
	cfgFile := ad.workdir + "/etc/lhsmd/agent"

	return cfgFile, ioutil.WriteFile(cfgFile, []byte(ad.ac.String()), 0644)
}

// AddSuiteConfig updates the driver's suite config
func (ad *AgentDriver) AddSuiteConfig(cfg *suite.Config) {
	ad.sc = cfg
}

// NewAgentDriver initializes a new AgentDriver instance with default values
func NewAgentDriver(workdir string, suiteConfig *suite.Config) *AgentDriver {
	return &AgentDriver{
		ac:      agent.NewConfig(),
		sc:      suiteConfig,
		workdir: workdir,
	}
}
