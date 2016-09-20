package lnet

import (
	"fmt"
	"net"

	"github.com/pkg/errors"
)

const o2IbDriverString = "o2ib"

func init() {
	drivers[o2IbDriverString] = newIbNid
}

// IbNid is an Infiniband LND NID
type IbNid struct {
	IPAddress      net.IP
	driverInstance int
}

// Address returns the underlying net.IP
// NB: This address is used for identification at the LND level, using
// the HCA port's IPoIB address.
func (t *IbNid) Address() interface{} {
	return t.IPAddress
}

// Driver returns the driver name
func (t *IbNid) Driver() string {
	return o2IbDriverString
}

// LNet returns a string representation of the driver name and instance
func (t *IbNid) LNet() string {
	return fmt.Sprintf("%s%d", t.Driver(), t.driverInstance)
}

func newIbNid(address string, driverInstance int) (RawNid, error) {
	ip := net.ParseIP(address)
	if ip == nil {
		return nil, errors.Errorf("%q is not a valid IP address", address)
	}
	return &IbNid{
		IPAddress:      ip,
		driverInstance: driverInstance,
	}, nil
}
