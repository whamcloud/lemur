package steps

import (
	"fmt"
	"os"
	"regexp"

	"github.com/pkg/errors"

	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/policy/pdm/uat/harness"
	"github.intel.com/hpdd/policy/pdm/uat/suite"
)

func init() {
	addStep(`^I (archive|restore|remove|release) (a test file|[\w\.\-/]+)$`, performHSMAction)
	addStep(`^(the test file|[\w\.\-/]+) should be marked as (archived|released)$`, checkFileStatus)
	addStep(`^the data for (the test file|[\w\.\-/]+) should exist in the backend$`, checkArchivedFileData)
}

func getFilePath(cfg *suite.Config, name string) (string, error) {
	newTestFileRe := regexp.MustCompile(`^a test file$`)
	curTestFileRe := regexp.MustCompile(`^the test file$`)
	if !newTestFileRe.MatchString(name) && !curTestFileRe.MatchString(name) {
		return os.Readlink(name)
	}

	if curTestFileRe.MatchString(name) {
		return ctx.GetKey(HSMTestFileKey)
	}

	return harness.CreateTestfile(ctx, cfg.LustrePath, HSMTestFileKey)
}

func performHSMAction(action, filename string) error {
	filePath, err := getFilePath(ctx.Config, filename)
	if err != nil {
		return errors.Wrapf(err, "Unable to get path for %s", filename)
	}

	switch action {
	case "archive":
		return ctx.HsmDriver.Archive(filePath)
	case "restore":
		return ctx.HsmDriver.Restore(filePath)
	case "remove":
		return ctx.HsmDriver.Remove(filePath)
	case "release":
		return ctx.HsmDriver.Release(filePath)
	default:
		return fmt.Errorf("Unknown HSM action %q", action)
	}
}

func checkArchivedFileData(filename string) error {
	// TODO: Compare md5sum of testfile and archived file?
	// How does this work for s3?
	return nil
}

func checkFileStatus(filename, status string) error {
	debug.Printf("filename: %s, status: %s", filename, status)
	filePath, err := getFilePath(ctx.Config, filename)
	if err != nil {
		return errors.Wrapf(err, "Unable to get path for %s", filename)
	}

	fileInDesiredState := func() error {
		hsmState, err := ctx.HsmDriver.GetState(filePath)
		debug.Printf("state: %s, err: %s", hsmState, err)
		if err != nil {
			return err
		}
		if hsmState.String() != status {
			return fmt.Errorf("wanted %s, got %s", status, hsmState)
		}

		return nil
	}

	return waitFor(fileInDesiredState, StatusUpdateTimeout)
}
