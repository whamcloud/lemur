// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package lnet

import "fmt"

const loDriverString = "lo"

func init() {
	drivers[loDriverString] = newLoopbackNid
}

// LoopbackNid is a Loopback LND NID. It will only ever be 0@lo.
type LoopbackNid struct{}

// Address returns the underlying net.IP
func (t *LoopbackNid) Address() interface{} {
	return "0"
}

// Driver returns the LND name
func (t *LoopbackNid) Driver() string {
	return loDriverString
}

// LNet returns a string representation of the driver name and instance
func (t *LoopbackNid) LNet() string {
	return fmt.Sprintf("%s", t.Driver())
}

func newLoopbackNid(address string, driverInstance int) (RawNid, error) {
	return &LoopbackNid{}, nil
}
