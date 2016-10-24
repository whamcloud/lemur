// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package main

import (
	"os"
	"path"
	"reflect"
	"testing"

	"github.com/intel-hpdd/lemur/cmd/lhsm-plugin-posix/posix"
	"github.com/intel-hpdd/lemur/cmd/lhsmd/config"
	"github.com/intel-hpdd/lemur/dmplugin"
	"github.com/intel-hpdd/lemur/internal/testhelpers"
	"github.com/intel-hpdd/lemur/pkg/fsroot"
)

func TestPosixLoadConfig(t *testing.T) {
	var cfg posixConfig
	cfgFile, cleanup := testhelpers.TempCopy(t, "./test-fixtures/lhsm-plugin-posix.test", 0600)
	defer cleanup()
	err := dmplugin.LoadConfig(cfgFile, &cfg)
	loaded := &cfg
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	expected := &posixConfig{
		NumThreads: 42,
		Archives: posix.ArchiveSet{
			&posix.ArchiveConfig{
				Name: "1",
				ID:   1,
				Root: "/tmp/archives/1",
			},
		},
	}

	if !reflect.DeepEqual(loaded, expected) {
		t.Fatalf("\nexpected: \n\n%#v\ngot: \n\n%#v\n\n", expected, loaded)
	}
}

func TestPosixInsecureConfig(t *testing.T) {
	var cfg posixConfig
	cfgFile, cleanup := testhelpers.TempCopy(t, "./test-fixtures/lhsm-plugin-posix.test", 0666)
	defer cleanup()

	err := dmplugin.LoadConfig(cfgFile, &cfg)
	if err == nil {
		t.Fatal("Used insecure file, expecteed error")
	}
	t.Log(err)
	// verify err is the correct error
}

func TestPosixMergedConfig(t *testing.T) {
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

	expected := &posixConfig{
		NumThreads: 42,
		Archives: posix.ArchiveSet{
			&posix.ArchiveConfig{
				Name: "1",
				ID:   1,
				Root: "/tmp/archives/1",
			},
		},
		Checksums: &posix.ChecksumConfig{},
	}

	if !reflect.DeepEqual(merged, expected) {
		t.Fatalf("\nexpected: \n\n%#v\ngot: \n\n%#v\n\n", expected, merged)
	}
}

func TestPosixArchiveValidation(t *testing.T) {
	var cfg posixConfig
	cfgFile, cleanup := testhelpers.TempCopy(t, "./test-fixtures/lhsm-plugin-posix.test", 0600)
	defer cleanup()
	err := dmplugin.LoadConfig(cfgFile, &cfg)
	loaded := &cfg
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	for _, archive := range loaded.Archives {
		if err := archive.CheckValid(); err != nil {
			t.Fatalf("err: %s", err)
		}
	}
}
func TestPosixArchiveValidation2(t *testing.T) {
	var cfg posixConfig
	cfgFile, cleanup := testhelpers.TempCopy(t, "./test-fixtures/lhsm-plugin-posix-badarchive", 0600)
	defer cleanup()
	err := dmplugin.LoadConfig(cfgFile, &cfg)
	loaded := &cfg
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	for _, archive := range loaded.Archives {
		if err := archive.CheckValid(); err == nil {
			t.Fatalf("expected %s to fail validation", archive)
		}
	}
}

func TestPosixChecksumConfig(t *testing.T) {
	var cfg posixConfig
	cfgFile, cleanup := testhelpers.TempCopy(t, "./test-fixtures/lhsm-plugin-posix.checksums", 0600)
	defer cleanup()
	err := dmplugin.LoadConfig(cfgFile, &cfg)
	loaded := &cfg
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	checksumConfigs := map[int]*posix.ChecksumConfig{
		0: &posix.ChecksumConfig{
			Disabled:                true,
			DisableCompareOnRestore: false,
		},
		1: &posix.ChecksumConfig{
			Disabled:                false,
			DisableCompareOnRestore: false,
		},
		2: &posix.ChecksumConfig{
			Disabled:                false,
			DisableCompareOnRestore: true,
		},
	}

	expected := &posixConfig{
		Archives: posix.ArchiveSet{
			&posix.ArchiveConfig{
				Name:      "1",
				ID:        1,
				Root:      "/tmp/archives/1",
				Checksums: checksumConfigs[1],
			},
			&posix.ArchiveConfig{
				Name:      "2",
				ID:        2,
				Root:      "/tmp/archives/2",
				Checksums: checksumConfigs[2],
			},
			&posix.ArchiveConfig{
				Name:      "3",
				ID:        3,
				Root:      "/tmp/archives/3",
				Checksums: nil,
			},
		},
		Checksums: checksumConfigs[0], // global
	}

	// First, ensure that the config was loaded as expected
	if !reflect.DeepEqual(loaded, expected) {
		t.Fatalf("\nexpected: \n\n%s\ngot: \n\n%s\n\n", expected, loaded)
	}

	posix.DefaultChecksums = *loaded.Checksums
	// Next, ensure that the archive backends are configured correctly
	var tests = []struct {
		archiveID   uint32
		expectedNum int
	}{
		{1, 1},
		{2, 2},
		{3, 0}, // should have the global config
	}

	getExpectedChecksum := func(id uint32) *posix.ChecksumConfig {
		for _, tc := range tests {
			if tc.archiveID == id {
				return checksumConfigs[tc.expectedNum]
			}
		}
		return nil
	}

	for _, a := range loaded.Archives {
		mover, err := posix.NewMover(a)
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		got := mover.ChecksumConfig()
		expected := getExpectedChecksum(uint32(a.ID))
		if !reflect.DeepEqual(expected, got) {
			t.Fatalf("\nexpected: \n\n%#v\ngot: \n\n%#v\n\n", expected, got)
		}
	}

}
