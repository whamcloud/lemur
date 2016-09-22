package main

import (
	"os"
	"path"
	"reflect"
	"testing"

	"github.intel.com/hpdd/lemur/cmd/lhsmd/config"
	"github.intel.com/hpdd/lemur/dmplugin"
	"github.intel.com/hpdd/lemur/internal/testhelpers"
	"github.intel.com/hpdd/lemur/pkg/fsroot"
)

func TestS3LoadConfig(t *testing.T) {
	var cfg s3Config
	cfgFile, cleanup := testhelpers.TempCopy(t, "./test-fixtures/lhsm-plugin-s3.test", 0600)
	defer cleanup()
	err := dmplugin.LoadConfig(cfgFile, &cfg)
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

func TestS3InsecureConfig(t *testing.T) {
	var cfg s3Config
	cfgFile, cleanup := testhelpers.TempCopy(t, "./test-fixtures/lhsm-plugin-s3.test", 0666)
	defer cleanup()

	err := dmplugin.LoadConfig(cfgFile, &cfg)
	if err == nil {
		t.Fatal("Used insecure file, expecteed error")
	}
	t.Log(err)
	/* verify err is the correct error */
}

func TestS3MergedConfig(t *testing.T) {
	os.Setenv(config.AgentConnEnvVar, "foo://bar:1234")
	os.Setenv(config.PluginMountpointEnvVar, "/foo/bar/baz")

	tmpDir, dirCleanup := testhelpers.TempDir(t)
	defer dirCleanup()

	testhelpers.CopyFile(t,
		path.Join("./test-fixtures", path.Base(os.Args[0])),
		path.Join(tmpDir, path.Base(os.Args[0])),
		0600)
	os.Setenv(config.ConfigDirEnvVar, tmpDir)

	plugin, err := dmplugin.New(path.Base(os.Args[0]), func(path string) (fsroot.Client, error) {
		return fsroot.Test(path), nil
	})
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
	cfgFile, cleanup := testhelpers.TempCopy(t, "./test-fixtures/lhsm-plugin-s3.test", 0600)
	defer cleanup()
	err := dmplugin.LoadConfig(cfgFile, &cfg)
	loaded := &cfg
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	for _, archive := range loaded.Archives {
		if err = archive.checkValid(); err != nil {
			t.Fatalf("err: %s", err)
		}
	}

	var cfg2 s3Config
	cfgFile2, cleanup2 := testhelpers.TempCopy(t, "./test-fixtures/lhsm-plugin-s3-badarchive", 0600)
	defer cleanup2()
	err = dmplugin.LoadConfig(cfgFile2, &cfg2)
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
