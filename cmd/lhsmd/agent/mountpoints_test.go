// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package agent

import (
	"reflect"
	"testing"

	"golang.org/x/sys/unix"

	"github.intel.com/hpdd/lemur/cmd/lhsmd/config"
	"github.intel.com/hpdd/lustre/fs/spec"
)

func TestMountConfigs(t *testing.T) {
	cfg, err := LoadConfig("./test-fixtures/plugin-config")
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	d, err := spec.ClientDeviceFromString("0@lo:/test")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	expectedDevice := d.String()
	expectedOptions := clientMountOptions{"user_xattr", "device=" + expectedDevice}
	var expectedFlags uintptr
	expectedFlags |= unix.MS_STRICTATIME
	expected := []*mountConfig{
		{
			Device:    expectedDevice,
			Directory: config.DefaultAgentMountRoot + "/agent",
			Type:      "lustre",
			Options:   expectedOptions,
			Flags:     expectedFlags,
		},
		{
			Device:    expectedDevice,
			Directory: config.DefaultAgentMountRoot + "/lhsm-plugin-posix",
			Type:      "lustre",
			Options:   expectedOptions,
			Flags:     expectedFlags,
		},
		{
			Device:    expectedDevice,
			Directory: config.DefaultAgentMountRoot + "/lhsm-plugin-s3",
			Type:      "lustre",
			Options:   expectedOptions,
			Flags:     expectedFlags,
		},
		{
			Device:    expectedDevice,
			Directory: config.DefaultAgentMountRoot + "/lhsm-plugin-noop",
			Type:      "lustre",
			Options:   expectedOptions,
			Flags:     expectedFlags,
		},
	}

	got := createMountConfigs(cfg)

	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("\nexpected:\n%s\ngot:\n%s\n", expected, got)
	}
}
