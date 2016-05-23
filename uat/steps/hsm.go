package steps

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"

	"github.com/pkg/errors"

	"github.intel.com/hpdd/logging/debug"
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

	// If we're not working with a specific file, then we'll need to
	// create one. We could use an empty file or fill it with zeros
	// or something, but that's no fun. Let's try copying the contents
	// of the test executable out as the HSM test file.
	out, err := ioutil.TempFile(cfg.LustrePath, HSMTestFileKey)
	if err != nil {
		return "", errors.Wrap(err, "Unable to create HSM test file")
	}

	// Won't work on OS X, but then again none of this will...
	srcPath, err := os.Readlink("/proc/self/exe")
	if err != nil {
		return "", errors.Wrap(err, "Unable to find path to self")
	}

	in, err := os.Open(srcPath)
	if err != nil {
		return "", errors.Wrapf(err, "Failed to open %s for read", srcPath)
	}
	defer in.Close()

	if _, err := bufio.NewReader(in).WriteTo(out); err != nil {
		return "", errors.Wrap(err, "Failed to write data to HSM test file")
	}

	ctx.SetKey(HSMTestFileKey, out.Name())
	debug.Printf("Created HSM test file: %s", out.Name())
	return out.Name(), out.Close()
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
