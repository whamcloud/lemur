package agent

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path"
	"time"

	"github.com/pkg/errors"

	"golang.org/x/net/context"

	"github.intel.com/hpdd/lemur/cmd/lhsmd/config"
	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/audit"
	"github.intel.com/hpdd/logging/debug"
)

var backoff = []time.Duration{
	0 * time.Second,
	1 * time.Second,
	10 * time.Second,
	30 * time.Second,
	1 * time.Minute,
}
var maxBackoff = len(backoff) - 1

type (
	// PluginConfig represents configuration for a single plugin
	PluginConfig struct {
		Name             string
		BinPath          string
		AgentConnection  string
		ClientMount      string
		Args             []string
		RestartOnFailure bool

		lastRestart  time.Time
		restartCount int
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
	data, err := json.Marshal(p)
	if err != nil {
		alert.Abort(errors.Wrap(err, "marshal failed"))
	}

	var out bytes.Buffer
	json.Indent(&out, data, "", "\t")
	return out.String()
}

// NoRestart optionally sets a plugin to not be restarted on failure
func (p *PluginConfig) NoRestart() *PluginConfig {
	p.RestartOnFailure = false
	return p
}

// RestartDelay returns a time.Duration to delay restarts based on
// the number of restarts and the last restart time.
func (p *PluginConfig) RestartDelay() time.Duration {
	// If it's been a decent amount of time since the last restart,
	// reset the backoff mechanism for a quick restart.
	if time.Since(p.lastRestart) > backoff[maxBackoff]*2 {
		p.restartCount = 0
	}

	if p.restartCount > maxBackoff {
		return backoff[maxBackoff]
	}
	return backoff[p.restartCount]
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
				delay := cfg.RestartDelay()
				audit.Logf("Restarting plugin %s after delay of %s (attempt %d)", cfg.Name, delay, cfg.restartCount)

				cfg.restartCount++
				cfg.lastRestart = time.Now()
				// Restart in a different goroutine to
				// avoid deadlocking this one.
				go func(cfg *PluginConfig, delay time.Duration) {
					<-time.After(delay)

					err := m.StartPlugin(cfg)
					if err != nil {
						audit.Logf("Failed to restart plugin %s: %s", cfg.Name, err)
					}
				}(cfg, delay)
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

	cmd.Env = append(os.Environ(), config.AgentConnEnvVar+"="+cfg.AgentConnection)
	cmd.Env = append(cmd.Env, config.PluginMountpointEnvVar+"="+cfg.ClientMount)

	if err := cmd.Start(); err != nil {
		return errors.Wrapf(err, "cmd failed %q", cmd)
	}

	audit.Logf("Started %s (PID: %d)", cmd.Path, cmd.Process.Pid)
	m.processChan <- &pluginProcess{cfg, cmd}

	return nil
}
