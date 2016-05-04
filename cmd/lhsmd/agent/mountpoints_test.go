package agent

import (
	"reflect"
	"testing"

	"golang.org/x/sys/unix"

	"github.intel.com/hpdd/ce-tools/resources/lustre/clientmount"
	"github.intel.com/hpdd/policy/pdm/lhsmd/config"
)

func TestMountConfigs(t *testing.T) {
	cfg, err := LoadConfig("./test-fixtures/plugin-config")
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	d, err := clientmount.ClientDeviceFromString("0@lo:/test")
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
