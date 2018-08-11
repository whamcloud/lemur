// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package status

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/intel-hpdd/go-lustre/llapi"

	yaml "gopkg.in/yaml.v2"
)

const (
	procBase = "/proc/fs/lustre"
)

// LustreClient is a local client
type LustreClient struct {
	FsName   string
	ClientID string
}

func (c *LustreClient) String() string {
	return c.FsName + "-" + c.ClientID
}

// Client returns the local Lustre client identifier for that mountpoint. This can
// be used to determine which entries in /proc/fs/lustre as associated with
// that client.
func Client(mountPath string) (*LustreClient, error) {
	id, err := llapi.GetName(mountPath)
	if err != nil {
		return nil, err
	}
	elem := strings.Split(id, "-")
	c := LustreClient{FsName: elem[0], ClientID: elem[1]}
	return &c, nil
}

func (c *LustreClient) getClientDevices(module string, cli string) []string {
	var ret []string
	nameGlob := fmt.Sprintf("%s*-%s-%s", c.FsName, cli, c.ClientID)
	p := filepath.Join(procBase, module, nameGlob)
	matches, _ := filepath.Glob(p)
	for _, c := range matches {
		ret = append(ret, clientName(c))
	}
	return ret
}

// ClientPath returns path to base proc file for a client module
func (c *LustreClient) ClientPath(module string, cli string) string {
	name := fmt.Sprintf("%s-%s-%s", cli, module, c.ClientID)
	p := filepath.Join(procBase, module, name)
	return p
}

func clientName(path string) string {
	imp, _ := ReadImport(path)
	t := imp.Target
	if strings.HasSuffix(t, "_UUID") {
		t = t[0 : len(t)-5]
	}
	return t
}

// LOVTargets retuns list of OSC devices in the LOV
func (c *LustreClient) LOVTargets() []string {
	return c.getClientDevices("osc", "osc")
}

// LMVTargets retuns list of MDC devices in the LMV
func (c *LustreClient) LMVTargets() []string {
	return c.getClientDevices("mdc", "mdc")
}

type (
	// Wrapper unnamed outer layer in import file
	Wrapper struct {
		Import Import
	}

	// Import is the state of a client import
	Import struct {
		Name         string
		State        string
		Target       string
		ConnectFlags []string `yaml:"connect_flags"`
		ImportFlags  []string `yaml:"import_flags"`
		Connection   ConnectionStatus
		// OSC only
		Averages WriteDataAverages `yaml:"write_data_averages"`
	}

	// WriteDataAverages is available on OSC imports
	WriteDataAverages struct {
		BytesPerRPC     int     `yaml:"bytes_per_rpc"`
		MicrosendPerRPC int     `yaml:"usec_per_rpc"`
		MegabytesPerSec float64 `yaml:"MB_per_sec"`
	}

	// ConnectionStatus is current status of the import's connection to the target.
	ConnectionStatus struct {
		FailoverNids            []string `yaml:"failover_nids"`
		CurrentConnection       string   `yaml:"current_connection"`
		ConnectionAttempts      int      `yaml:"connection_attempts"`
		Generation              int      `yaml:"generation"`
		InProgressInvalidations int      `yaml:"in-progress_invalidations"`
	}
)

func removeZeros(b []byte) []byte {
	var copy = make([]byte, 0)
	for _, n := range b {
		if n != 0 {
			copy = append(copy, n)
		}
	}
	return copy
}

// ReadImport returns Import from the given client dir.
func ReadImport(path string) (*Import, error) {
	result := Wrapper{}
	b := make([]byte, 8192)
	importPath := filepath.Join(path, "import")

	// ioutil.ReadFile chokes on the binary data embedded in import (LU-5567)
	fp, err := os.Open(importPath)
	if err != nil {
		return nil, err
	}
	_, err = fp.Read(b)
	if err != nil {
		return nil, err
	}
	b = removeZeros(b) // sanitize
	e := yaml.Unmarshal(b, &result)
	if e != nil {
		return nil, e
	}
	return &result.Import, nil
}
