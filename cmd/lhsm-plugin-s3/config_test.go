// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package main

import (
	"os"
	"path"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/intel-hpdd/lemur/cmd/lhsmd/config"
	"github.com/intel-hpdd/lemur/dmplugin"
	"github.com/intel-hpdd/lemur/internal/testhelpers"
	"github.com/intel-hpdd/lemur/pkg/fsroot"
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
				Name:           "2",
				ID:             2,
				Region:         "us-west-1",
				Bucket:         "hpdd-test-bucket",
				Prefix:         "archive-test",
				UploadPartSize: 16,
			},
		},
	}

	if !reflect.DeepEqual(loaded, expected) {
		t.Fatalf("\nexpected: \n\n%s\ngot: \n\n%s\n\n", expected, loaded)
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
	// verify err is the correct error
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
		Region:         "us-east-1",
		UploadPartSize: s3manager.DefaultUploadPartSize,
		Archives: archiveSet{
			&archiveConfig{
				Name:           "2",
				ID:             2,
				Region:         "us-west-1",
				Bucket:         "hpdd-test-bucket",
				Prefix:         "archive-test",
				UploadPartSize: 16,
			},
		},
	}

	if !reflect.DeepEqual(merged, expected) {
		t.Fatalf("\nexpected: \n\n%s\ngot: \n\n%s\n\n", expected, merged)
	}
}

func TestArchiveValidation(t *testing.T) {
	cfg := &s3Config{}
	cfgFile, cleanup := testhelpers.TempCopy(t, "./test-fixtures/lhsm-plugin-s3.test", 0600)
	defer cleanup()
	err := dmplugin.LoadConfig(cfgFile, cfg)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	for _, archive := range cfg.Archives {
		archive.mergeGlobals(cfg)
		if err = archive.checkValid(); err != nil {
			t.Fatalf("err: %s", err)
		}
	}

	cfg2 := &s3Config{}
	cfgFile2, cleanup2 := testhelpers.TempCopy(t, "./test-fixtures/lhsm-plugin-s3-badarchive", 0600)
	defer cleanup2()
	err = dmplugin.LoadConfig(cfgFile2, cfg2)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	for _, archive := range cfg2.Archives {
		archive.mergeGlobals(cfg)
		if err := archive.checkValid(); err == nil {
			t.Fatalf("expected %s to fail validation", archive)
		}
	}
}
