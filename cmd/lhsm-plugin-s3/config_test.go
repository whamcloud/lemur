package main

import (
	"os"
	"path"
	"reflect"
	"testing"

	"github.intel.com/hpdd/lemur/cmd/lhsmd/config"
	"github.intel.com/hpdd/lemur/dmplugin"
)

func TestLoadConfig(t *testing.T) {
	var cfg s3Config
	err := dmplugin.LoadConfig("./test-fixtures/lhsm-plugin-s3.test", &cfg)
	loaded := &cfg
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	expected := &s3Config{
		Archives: archiveSet{
			&archiveConfig{
				Name:   "2",
				ID:     2,
				Region: "us-west-1",
				Bucket: "hpdd-test-bucket",
				Prefix: "archive-test",
			},
		},
	}

	if !reflect.DeepEqual(loaded, expected) {
		t.Fatalf("\nexpected: \n\n%#v\ngot: \n\n%#v\n\n", expected, loaded)
	}
}

func TestMergedConfig(t *testing.T) {
	os.Setenv(config.AgentConnEnvVar, "foo://bar:1234")
	os.Setenv(config.PluginMountpointEnvVar, "/foo/bar/baz")
	os.Setenv(config.ConfigDirEnvVar, "./test-fixtures")

	plugin, err := dmplugin.NewTestPlugin(path.Base(os.Args[0]))
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	merged, err := getMergedConfig(plugin)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	expected := &s3Config{
		Region: "us-east-1",
		Archives: archiveSet{
			&archiveConfig{
				Name:   "2",
				ID:     2,
				Region: "us-west-1",
				Bucket: "hpdd-test-bucket",
				Prefix: "archive-test",
			},
		},
	}

	if !reflect.DeepEqual(merged, expected) {
		t.Fatalf("\nexpected: \n\n%#v\ngot: \n\n%#v\n\n", expected, merged)
	}
}

func TestArchiveValidation(t *testing.T) {
	var cfg s3Config
	err := dmplugin.LoadConfig("./test-fixtures/lhsm-plugin-s3.test", &cfg)
	loaded := &cfg
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	for _, archive := range loaded.Archives {
		if err := archive.checkValid(); err != nil {
			t.Fatalf("err: %s", err)
		}
	}

	var cfg2 s3Config
	err = dmplugin.LoadConfig("./test-fixtures/lhsm-plugin-s3-badarchive", &cfg2)
	loaded = &cfg2
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	for _, archive := range loaded.Archives {
		if err := archive.checkValid(); err == nil {
			t.Fatalf("expected %s to fail validation", archive)
		}
	}
}
