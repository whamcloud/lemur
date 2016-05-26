package harness

import (
	"bufio"
	"io/ioutil"
	"os"

	"github.com/pkg/errors"
	"github.intel.com/hpdd/logging/debug"
)

// CreateTestfile creates a test file and adds its path to the context's
// cleanup queue.
func CreateTestfile(ctx *ScenarioContext, prefix, key string) (string, error) {
	// If we're not working with a specific file, then we'll need to
	// create one. We could use an empty file or fill it with zeros
	// or something, but that's no fun. Let's try copying the contents
	// of the test executable out as the HSM test file.
	out, err := ioutil.TempFile(prefix, key)
	if err != nil {
		return "", errors.Wrap(err, "Unable to create test file")
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

	ctx.AddCleanup(func() error {
		return os.Remove(out.Name())
	})
	ctx.SetKey(key, out.Name())
	debug.Printf("Created test file: %s", out.Name())
	return out.Name(), out.Close()
}
