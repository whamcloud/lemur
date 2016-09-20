package spec

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/pkg/errors"

	"github.intel.com/hpdd/lustre/lnet"
)

// ClientDevice represents a mgsspec:/fsname client mount device
type ClientDevice struct {
	MgsSpec lnet.TargetSpec
	FsName  string
}

func (d *ClientDevice) String() string {
	return fmt.Sprintf("%s:/%s", d.MgsSpec, d.FsName)
}

// MarshalJSON implements json.Marshaler
func (d *ClientDevice) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

// UnmarshalJSON implements json.Unmarshaler
func (d *ClientDevice) UnmarshalJSON(data []byte) error {
	var devStr string
	if err := json.Unmarshal(data, &devStr); err != nil {
		return errors.Wrap(err, "unmarshal failed")
	}
	dev, err := ClientDeviceFromString(devStr)
	if err != nil {
		return errors.Wrap(err, "parsing client device failed")
	}
	*d = *dev
	return nil
}

// ClientDeviceFromString attempts to parse a device out of a string
// and returns an instance of ClientDevice if successful
func ClientDeviceFromString(inString string) (*ClientDevice, error) {
	devRe := regexp.MustCompile(`^(.+):/([^:/]+)$`)
	matches := devRe.FindStringSubmatch(inString)
	if len(matches) < 3 {
		return nil, errors.Errorf("Cannot parse client mount device from %q", inString)
	}

	dev := &ClientDevice{
		FsName: matches[2],
	}

	for _, nodeStr := range strings.Split(matches[1], ":") {
		var nidList lnet.NidList
		for _, nidStr := range strings.Split(nodeStr, ",") {
			nid, err := lnet.NidFromString(nidStr)
			if err != nil {
				return nil, errors.Wrap(err, "parsing nid failed")
			}
			nidList = append(nidList, nid)
		}
		dev.MgsSpec = append(dev.MgsSpec, nidList)
	}

	return dev, nil
}
