package steps

import (
	"fmt"

	"github.com/pkg/errors"
	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/pkg/mntent"
)

func init() {
	addStep(`^I have a Lustre filesystem$`, Context.iHaveALustreFilesystem)
}

func (sc *stepContext) iHaveALustreFilesystem() error {
	if sc.SuiteConfig.LustrePath != "" {
		if _, err := fs.MountRoot(sc.SuiteConfig.LustrePath); err != nil {
			return fmt.Errorf("Configured Lustre path is invalid: %s", err)
		}
		return nil
	}

	entries, err := mntent.GetEntriesByType("lustre")
	if err != nil {
		return errors.Wrap(err, "Failed to get Lustre mounts")
	}

	for _, entry := range entries {
		if _, err := fs.MountRoot(entry.Dir); err == nil {
			sc.SuiteConfig.LustrePath = entry.Dir
			return nil
		}
	}

	return fmt.Errorf("No Lustre filesystem found")
}
