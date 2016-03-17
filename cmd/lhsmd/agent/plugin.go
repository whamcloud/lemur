package agent

import (
	"fmt"
	"os"
	"os/exec"
	"path"

	"golang.org/x/net/context"

	"github.intel.com/hpdd/logging/audit"
	"github.intel.com/hpdd/logging/debug"
)

const (
	// AgentConnEnvVar is the environment variable containing a connect
	// string for plugins to use when registering with the agent
	AgentConnEnvVar = "LHSMD_AGENT_CONNECTION"

	// PluginMountpointEnvVar is the environment variable containing
	// a Lustre client mountpoint to be used by the plugin
	PluginMountpointEnvVar = "LHSMD_CLIENT_MOUNTPOINT"
)

type (
	// PluginConfig represents configuration for a single plugin
	PluginConfig struct {
		Name             string
		BinPath          string
		AgentConnection  string
		ClientMount      string
		Args             []string
		RestartOnFailure bool
	}

	// PluginMonitor watches monitored plugins and restarts
	// them as needed.
	PluginMonitor struct {
		processChan      ppChan
		processStateChan psChan
	}

	pluginProcess struct {
		plugin *PluginConfig
		cmd    *exec.Cmd
	}

	pluginStatus struct {
		ps  *os.ProcessState
		err error
	}

	ppChan chan *pluginProcess
	psChan chan *pluginStatus
)

func (p *PluginConfig) String() string {
	return fmt.Sprintf("%s (%s): %s", p.Name, p.BinPath, p.Args)
}

// NoRestart optionally sets a plugin to not be restarted on failure
func (p *PluginConfig) NoRestart() *PluginConfig {
	p.RestartOnFailure = false
	return p
}

// NewPlugin returns a plugin configuration
func NewPlugin(name, binPath, conn, mountRoot string, args ...string) *PluginConfig {
	return &PluginConfig{
		Name:             name,
		BinPath:          binPath,
		AgentConnection:  conn,
		ClientMount:      path.Join(mountRoot, name),
		Args:             args,
		RestartOnFailure: true,
	}
}

// NewMonitor creates a new plugin monitor
func NewMonitor() *PluginMonitor {
	return &PluginMonitor{
		processChan:      make(ppChan),
		processStateChan: make(psChan),
	}
}

func (m *PluginMonitor) run(ctx context.Context) {
	processMap := make(map[int]*PluginConfig)

	var waitForCmd = func(cmd *exec.Cmd) {
		debug.Printf("Waiting for %s (%d) to exit", cmd.Path, cmd.Process.Pid)
		ps, err := cmd.Process.Wait()
		if err != nil {
			audit.Logf("Err after Wait() for %d: %s", cmd.Process.Pid, err)
		}

		debug.Printf("PID %d finished: %s", cmd.Process.Pid, ps)
		m.processStateChan <- &pluginStatus{ps, err}
	}

	for {
		select {
		case p := <-m.processChan:
			processMap[p.cmd.Process.Pid] = p.plugin
			go waitForCmd(p.cmd)
		case s := <-m.processStateChan:
			cfg, found := processMap[s.ps.Pid()]
			if !found {
				debug.Printf("Received disp of unknown pid: %d", s.ps.Pid())
				break
			}

			delete(processMap, s.ps.Pid())
			audit.Logf("Process %d for %s died: %s", s.ps.Pid(), cfg.Name, s.ps)
			if cfg.RestartOnFailure {
				// FIXME: This needs some kind of mechanism
				// to prevent endless restarts of a
				// badly-configured plugin!!!
				audit.Logf("Restarting plugin: %s", cfg.Name)
				// Restart in a different goroutine to
				// avoid deadlocking this one.
				go func(cfg *PluginConfig) {
					err := m.StartPlugin(cfg)
					if err != nil {
						audit.Logf("Failed to restart plugin %s: %s", cfg.Name, err)
					}
				}(cfg)
			}
		case <-ctx.Done():
			return
		}
	}
}

// Start creates a new plugin monitor
func (m *PluginMonitor) Start(ctx context.Context) {
	go m.run(ctx)
}

// StartPlugin starts the plugin and monitors it
func (m *PluginMonitor) StartPlugin(cfg *PluginConfig) error {
	debug.Printf("Starting %s for %s", cfg.BinPath, cfg.Name)

	cmd := exec.Command(cfg.BinPath, cfg.Args...)

	prefix := path.Base(cfg.BinPath)
	cmd.Stdout = audit.Writer().Prefix(prefix + " ")
	cmd.Stderr = audit.Writer().Prefix(prefix + "-stderr ")

	cmd.Env = append(os.Environ(), AgentConnEnvVar+"="+cfg.AgentConnection)
	cmd.Env = append(cmd.Env, PluginMountpointEnvVar+"="+cfg.ClientMount)

	if err := cmd.Start(); err != nil {
		return err
	}

	audit.Logf("Started %s (PID: %d)", cmd.Path, cmd.Process.Pid)
	m.processChan <- &pluginProcess{cfg, cmd}

	return nil
}
