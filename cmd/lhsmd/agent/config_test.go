package agent

import (
	"reflect"
	"runtime"
	"testing"

	"github.intel.com/hpdd/ce-tools/resources/lustre/clientmount"
)

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
	}

	if !reflect.DeepEqual(loaded, expected) {
		t.Fatalf("\nexpected:\n%#v\ngot:\n%#v\n", expected, loaded)
	}
}

func TestMergedConfig(t *testing.T) {
	defCfg := &Config{
		Processes: 2,
		InfluxDB:  &influxConfig{},
		ClientMountOptions: []string{
			"user_xattr",
		},
		EnabledPlugins: []string{
			"lhsm-plugin-noop",
		},
		PluginDir: "/usr/share/lhsmd/plugins",
		Snapshots: &snapshotConfig{
			Enabled: true,
		},
		Transport: &transportConfig{
			Type: "grpc",
			Port: 9000,
		},
	}

	loaded, err := LoadConfig("./test-fixtures/good-config")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	loaded = defCfg.Merge(loaded)

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

	if !reflect.DeepEqual(loaded, expected) {
		t.Fatalf("\nexpected:\n%s\ngot:\n%s\n", expected, loaded)
	}
}
