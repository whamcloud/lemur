package drivers

import (
	"fmt"
	"os/exec"
)

type driverConstructor func() HsmDriver

// PreferredHsmDrivers is a map of HSM driver names to their
// driver constructor functions.
var PreferredHsmDrivers = map[string]driverConstructor{"lfs": LfsDriver}

// NewMultiHsmDriver returns an implementation of HsmDriver which
// finds an available HSM driver according to the preferred order.
func NewMultiHsmDriver() HsmDriver {
	return &multiHsmDriver{
		delegate: findDelegate(),
	}
}

func findDelegate() HsmDriver {
	for name, constructor := range PreferredHsmDrivers {
		if _, err := exec.LookPath(name); err == nil {
			return constructor()
		}
	}

	return &failedDelegate{}
}

type failedDelegate struct{}

func (f *failedDelegate) fail(action string) error {
	return fmt.Errorf("Unable to delegate %s action: No HSM drivers found.", action)
}

func (f *failedDelegate) Archive(filePath string) error {
	return f.fail("archive")
}

func (f *failedDelegate) Release(filePath string) error {
	return f.fail("release")
}

func (f *failedDelegate) Restore(filePath string) error {
	return f.fail("restore")
}

func (f *failedDelegate) Remove(filePath string) error {
	return f.fail("remove")
}

func (f *failedDelegate) GetState(filePath string) (HsmState, error) {
	return Unknown, f.fail("get file state")
}

type multiHsmDriver struct {
	delegate HsmDriver
}

func (d *multiHsmDriver) Archive(filePath string) error {
	return d.delegate.Archive(filePath)
}

func (d *multiHsmDriver) Restore(filePath string) error {
	return d.delegate.Restore(filePath)
}

func (d *multiHsmDriver) Remove(filePath string) error {
	return d.delegate.Remove(filePath)
}

func (d *multiHsmDriver) Release(filePath string) error {
	return d.delegate.Release(filePath)
}

func (d *multiHsmDriver) GetState(filePath string) (HsmState, error) {
	return d.delegate.GetState(filePath)
}
