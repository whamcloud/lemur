// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package lnet

import (
	"encoding/json"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// Each driver implementation needs to register itself in this map.
var drivers = make(map[string]newNidFunc)

type (
	newNidFunc func(string, int) (RawNid, error)

	// RawNid represents the actual Nid implementation (tcp, o2ib, etc)
	RawNid interface {
		Driver() string
		Address() interface{}
		LNet() string
	}

	// Nid is a container for RawNid and holds methods for serializing
	// to/from JSON.
	Nid struct {
		raw RawNid
	}
)

// MarshalJSON implements json.Marshaler
func (nid *Nid) MarshalJSON() ([]byte, error) {
	return json.Marshal(nid.String())
}

// UnmarshalJSON implements json.Unmarshaler
func (nid *Nid) UnmarshalJSON(b []byte) error {
	var nidStr string
	if err := json.Unmarshal(b, &nidStr); err != nil {
		return err
	}
	n, err := NidFromString(nidStr)
	if err != nil {
		return err
	}
	*nid = *n
	return nil
}

func (nid *Nid) String() string {
	return fmt.Sprintf("%s@%s", nid.raw.Address(), nid.raw.LNet())
}

// Address returns the underlying Nid address (e.g. a net.IP, string, etc.)
func (nid *Nid) Address() interface{} {
	return nid.raw.Address()
}

// Driver returns the name of the Nid's LND
func (nid *Nid) Driver() string {
	return nid.raw.Driver()
}

// SupportedDrivers returns a list of supported LND names
func SupportedDrivers() []string {
	var list []string
	for driver := range drivers {
		list = append(list, driver)
	}
	return list
}

// NidFromString takes a string representation of a Nid and returns an
// *Nid.
func NidFromString(inString string) (*Nid, error) {
	nidRe := regexp.MustCompile(`^(.+)@(\w+[^\d*])(\d*)$`)
	matches := nidRe.FindStringSubmatch(inString)
	if len(matches) < 3 {
		return nil, errors.Errorf("Cannot parse NID from %q", inString)
	}

	address := matches[1]
	driver := matches[2]
	var driverInstance int
	if matches[3] != "" {
		var err error
		driverInstance, err = strconv.Atoi(matches[3])
		if err != nil {
			return nil, errors.Wrap(err, "driver instance number failed")
		}
	}

	ipRe := regexp.MustCompile(`^[\.\d]+`)
	if !ipRe.MatchString(address) {
		// Not an IP address or number; try to resolve it
		ipAddrs, err := net.LookupIP(address)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to resolve non-numeric address %q into an IP address", address)
		}

		// Just take the first IPv4 address (LNet doesn't support IPv6)
		for _, addr := range ipAddrs {
			if addr.To4() != nil {
				address = addr.String()
				break
			}
		}
	}

	if initFunc, present := drivers[driver]; present {
		raw, err := initFunc(address, driverInstance)
		if err != nil {
			return nil, errors.Wrap(err, "nid init failed")
		}
		return &Nid{raw: raw}, nil
	}
	return nil, errors.Errorf("Unsupported LND: %s", driver)
}

// NidList is a list of NIDs for a server
type NidList []*Nid

func (n NidList) String() string {
	var nidStrings []string
	for _, nid := range n {
		nidStrings = append(nidStrings, nid.String())
	}
	return strings.Join(nidStrings, ",")
}

// TargetSpec is a list of NidLists -- any of the NIDs could be used
// to access this target on one of its servers
type TargetSpec []NidList

func (l TargetSpec) String() string {
	var nodeStrings []string
	for _, node := range l {
		nodeStrings = append(nodeStrings, node.String())
	}
	return strings.Join(nodeStrings, ":")
}
