// Copyright (c) 2016 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package agent

import (
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"runtime"
	"testing"

	"github.com/intel-hpdd/lemur/cmd/lhsmd/config"
	"github.com/intel-hpdd/go-lustre/fs/spec"
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
			AgentConnection:  "",
			ClientMount:      "/mnt/lhsmd/lhsm-plugin-posix",
			RestartOnFailure: true,
		},
		{
			Name:             "lhsm-plugin-s3",
			BinPath:          config.DefaultPluginDir + "/lhsm-plugin-s3",
			AgentConnection:  "",
			ClientMount:      "/mnt/lhsmd/lhsm-plugin-s3",
			RestartOnFailure: true,
		},
		{
			Name:             "lhsm-plugin-noop",
			BinPath:          config.DefaultPluginDir + "/lhsm-plugin-noop",
			AgentConnection:  "",
			ClientMount:      "/mnt/lhsmd/lhsm-plugin-noop",
			RestartOnFailure: true,
		},
	}
	t.Skip("TODO: Fix test to deal with unix socket")
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

	expectedDevice, err := spec.ClientDeviceFromString("10.211.55.37@tcp0:/testFs")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	expected := &Config{
		MountRoot:    "/mnt/lhsmd",
		ClientDevice: expectedDevice,
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
			Type:      "grpc",
			SocketDir: "/tmp",
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

	expectedDevice, err := spec.ClientDeviceFromString("10.211.55.37@tcp0:/testFs")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	expected := &Config{
		MountRoot:    "/mnt/lhsmd",
		ClientDevice: expectedDevice,
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
		PluginDir: "/go/bin",
		Snapshots: &snapshotConfig{
			Enabled: false,
		},
		Transport: &transportConfig{
			Type:      "grpc",
			SocketDir: "/var/run/lhsmd",
		},
	}

	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("\nexpected:\n%s\ngot:\n%s\n", expected, got)
	}
}

func TestJsonConfig(t *testing.T) {
	cfg, err := LoadConfig("./test-fixtures/json-config")

	if err != nil {
		t.Fatalf("Error from LoadConfig(): %s", err)
	}

	if cfg.ClientDevice == nil {
		t.Fatal("ClientDevice should not be nil")
	}
}

func TestConfigSaveLoad(t *testing.T) {
	startCfg := DefaultConfig()
	cd, err := spec.ClientDeviceFromString("1.2.3.4@tcp:/foo")
	if err != nil {
		t.Fatal(err)
	}
	startCfg.ClientDevice = cd

	td, err := ioutil.TempDir("", "agent-config-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(td)

	cfgFile := path.Join(td, "cfg")
	if err = ioutil.WriteFile(cfgFile, []byte(startCfg.String()), 0644); err != nil {
		t.Fatal(err)
	}

	loaded, err := LoadConfig(cfgFile)
	if err != nil {
		t.Fatal(err)
	}

	if startCfg.String() != loaded.String() {
		t.Fatalf("start cfg != loaded\nstart:\n%s\nloaded:\n%s\n", startCfg, loaded)
	}
}
