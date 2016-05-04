package agent_test

import (
	"flag"
	"fmt"
	"testing"

	"github.com/fortytw2/leaktest"

	"github.intel.com/hpdd/policy/pdm/lhsmd/agent"
	_ "github.intel.com/hpdd/policy/pdm/lhsmd/transport/grpc"
	"github.intel.com/hpdd/policy/pkg/client"

	"golang.org/x/net/context"
)

var testConfig string

func init() {
	flag.StringVar(&testConfig, "tc", "", "path to test agent config")
	flag.Parse()
}

func LoadTestConfig() (*agent.Config, error) {
	if testConfig == "" {
		return nil, fmt.Errorf("Unable to load test agent config file")
	}

	return agent.LoadConfig(testConfig)
}

func TestAgentStartStop(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := agent.DefaultConfig()
	cfg.Transport.Port = 12345
	cl := client.Test("/tmp/foo")
	a, err := agent.New(cfg, cl)
	if err != nil {
		t.Fatalf("error creating agent: %s", err)
	}

	ctx := context.Background()
	go func() {
		if err := a.Start(ctx); err != nil {
			t.Fatalf("error starting agent: %s", err)
		}
	}()

	a.Stop()
}
