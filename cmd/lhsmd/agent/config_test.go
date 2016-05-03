package agent

import (
	"reflect"
	"runtime"
	"testing"

	"github.intel.com/hpdd/ce-tools/resources/lustre/clientmount"
	"github.intel.com/hpdd/policy/pdm/lhsmd/config"
)

func TestConfiguredPlugins(t *testing.T) {
	loaded, err := LoadConfig("./test-fixtures/plugin-config")
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	expected := []*PluginConfig{
		{
			Name:             "lhsm-plugin-posix",
			BinPath:          config.DefaultPluginDir + "/lhsm-plugin-posix",
			AgentConnection:  ":4242",
			ClientMount:      "/mnt/lhsmd/lhsm-plugin-posix",
			RestartOnFailure: true,
		},
		{
			Name:             "lhsm-plugin-s3",
			BinPath:          config.DefaultPluginDir + "/lhsm-plugin-s3",
			AgentConnection:  ":4242",
			ClientMount:      "/mnt/lhsmd/lhsm-plugin-s3",
			RestartOnFailure: true,
		},
		{
			Name:             "lhsm-plugin-noop",
			BinPath:          config.DefaultPluginDir + "/lhsm-plugin-noop",
			AgentConnection:  ":4242",
			ClientMount:      "/mnt/lhsmd/lhsm-plugin-noop",
			RestartOnFailure: true,
		},
	}

	got := loaded.Plugins()
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("\nexpected:\n%s\ngot:\n%s\n", expected, got)
	}
}

func TestLoadConfig(t *testing.T) {
	loaded, err := LoadConfig("./test-fixtures/good-config")
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	expectedDevice, err := clientmount.ClientDeviceFromString("10.211.55.37@tcp0:/testFs")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	expected := &Config{
		MountRoot:       "/mnt/lhsmd",
		AgentMountpoint: "/mnt/lhsmd/agent",
		ClientDevice:    expectedDevice,
		ClientMountOptions: []string{
			"user_xattr",
		},
		Processes: runtime.NumCPU(),
		InfluxDB: &influxConfig{
			URL: "http://172.17.0.4:8086",
			DB:  "lhsmd",
		},
		EnabledPlugins: []string{
			"lhsm-plugin-posix",
		},
		Snapshots: &snapshotConfig{
			Enabled: false,
		},
		PluginDir: "/go/bin",
		Transport: &transportConfig{
			Type: "grpc",
			Port: 4242,
		},
	}

	if !reflect.DeepEqual(loaded, expected) {
		t.Fatalf("\nexpected:\n%s\ngot:\n%s\n", expected, loaded)
	}
}

func TestMergedConfig(t *testing.T) {
	defCfg := NewConfig()
	loaded, err := LoadConfig("./test-fixtures/merge-config")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	got := defCfg.Merge(loaded)

	expectedDevice, err := clientmount.ClientDeviceFromString("10.211.55.37@tcp0:/testFs")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	expected := &Config{
		MountRoot:       "/mnt/lhsmd",
		AgentMountpoint: "/mnt/lhsmd/agent",
		ClientDevice:    expectedDevice,
		ClientMountOptions: []string{
			"user_xattr",
		},
		Processes: 2,
		InfluxDB: &influxConfig{
			URL: "http://172.17.0.4:8086",
			DB:  "lhsmd",
		},
		EnabledPlugins: []string{
			"lhsm-plugin-posix",
		},
		PluginDir: "/go/bin",
		Snapshots: &snapshotConfig{
			Enabled: false,
		},
		Transport: &transportConfig{
			Type: "grpc",
			Port: 9000,
		},
	}

	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("\nexpected:\n%s\ngot:\n%s\n", expected, got)
	}
}
