package steps

import (
	"os"

	"github.com/pkg/errors"

	"github.intel.com/hpdd/lemur/uat/harness"
	"github.intel.com/hpdd/logging/debug"
)

func init() {
	addStep(`^I (archive|restore|remove|release) (a test file|the test file|[\w\.\-/]+)$`, performHSMAction)
	addStep(`^I have (archived|restored|removed|released) (a test file|the test file|[\w\.\-/]+)$`, performAndCheckHSMAction)
	addStep(`^(the test file|[\w\.\-/]+) should be marked as (unmanaged|unarchived|archived|released)$`, checkFileStatus)
	addStep(`^the data for (the test file|[\w\.\-/]+) should be (archived|restored|removed)$`, checkFileData)
}

var newTestFile = "a test file"
var curTestFile = "the test file"

func getFilePath(name string) (string, error) {
	if name != newTestFile && name != curTestFile {
		return os.Readlink(name)
	}

	if name == curTestFile {
		tf, err := getHsmTestFile()
		if err != nil {
			return "", err
		}
		return tf.Path, nil
	}

	return ctx.CreateTestfile(ctx.Config.LustrePath, HSMTestFileKey)
}

func performHSMAction(action, filename string) error {
	filePath, err := getFilePath(filename)
	if err != nil {
		return errors.Wrapf(err, "Unable to get path for %s", filename)
	}

	switch action {
	case "archive", "archived":
		return ctx.HsmDriver.Archive(filePath)
	case "restore", "restored":
		return ctx.HsmDriver.Restore(filePath)
	case "remove", "removed":
		return ctx.HsmDriver.Remove(filePath)
	case "release", "released":
		return ctx.HsmDriver.Release(filePath)
	default:
		return errors.Errorf("Unknown HSM action %q", action)
	}
}

func performAndCheckHSMAction(action, filename string) error {
	if err := performHSMAction(action, filename); err != nil {
		return errors.Wrap(err, "Failed to perform HSM action")
	}

	if filename == newTestFile {
		filename = curTestFile
	}
	return checkFileStatus(filename, action)
}

func getHsmTestFile() (*harness.TestFile, error) {
	tf, ok := ctx.TestFiles[HSMTestFileKey]
	if !ok {
		return nil, errors.Errorf("No HSM test file was registered with context")
	}

	return tf, nil
}

func checkFileData(filename, state string) error {
	// TODO: Check the data state in the archive for the archived and
	// removed states.
	switch state {
	case "restored":
		tf, err := getHsmTestFile()
		if err != nil {
			return err
		}

		newSum, err := harness.GetFileChecksum(tf.Path)
		if err != nil {
			return errors.Wrap(err, "Unable to get checksum for restored file")
		}

		debug.Printf("original: %x, restored: %x", tf.Checksum, newSum)
		if newSum != tf.Checksum {
			return errors.Errorf("Restored checksum does not match original checksum (%x != %x)", newSum, tf.Checksum)
		}
	}

	return nil
}

func checkFileStatus(filename, status string) error {
	debug.Printf("filename: %s, status: %s", filename, status)
	filePath, err := getFilePath(filename)
	if err != nil {
		return errors.Wrapf(err, "Unable to get path for %s", filename)
	}

	fileInDesiredState := func() error {
		hsmState, err := ctx.HsmDriver.GetState(filePath)
		debug.Printf("desired: %s, actual: %s (%v)", status, hsmState, err)
		if err != nil {
			return errors.Wrap(err, "GetState failed")
		}
		if hsmState.String() != status {
			return errors.Errorf("wanted %s, got %s", status, hsmState)
		}

		return nil
	}

	return waitFor(fileInDesiredState, StatusUpdateTimeout)
}
