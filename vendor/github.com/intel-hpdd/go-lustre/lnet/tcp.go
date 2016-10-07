// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package lnet

import (
	"fmt"
	"net"

	"github.com/pkg/errors"
)

const tcpDriverString = "tcp"

func init() {
	drivers[tcpDriverString] = newTCPNid
}

// TCPNid is a TCP LND NID
type TCPNid struct {
	IPAddress      net.IP
	driverInstance int
}

// Address returns the underlying net.IP
func (t *TCPNid) Address() interface{} {
	return t.IPAddress
}

// Driver returns the LND name
func (t *TCPNid) Driver() string {
	return tcpDriverString
}

// LNet returns a string representation of the driver name and instance
func (t *TCPNid) LNet() string {
	return fmt.Sprintf("%s%d", t.Driver(), t.driverInstance)
}

func newTCPNid(address string, driverInstance int) (RawNid, error) {
	ip := net.ParseIP(address)
	if ip == nil {
		return nil, errors.Errorf("%q is not a valid IP address", address)
	}
	return &TCPNid{
		IPAddress:      ip,
		driverInstance: driverInstance,
	}, nil
}
