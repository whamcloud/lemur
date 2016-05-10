package posix_test

import (
	"io/ioutil"
	"os"
	"testing"
)

var testPrefix = "ptest"

func testTempDir(t *testing.T) (string, func()) {
	tdir, err := ioutil.TempDir("", testPrefix)
	if err != nil {
		t.Fatal(err)
	}
	return tdir, func() {
		err = os.RemoveAll(tdir)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func testChdirTemp(t *testing.T) func() {
	tdir, cleanDir := testTempDir(t)

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	err = os.Chdir(tdir)
	if err != nil {
		t.Fatal(err)
	}

	return func() {
		err := os.Chdir(cwd)
		if err != nil {
			t.Fatal(err)
		}
		cleanDir()
	}
}

func testFill(t *testing.T, fp *os.File, size uint64) {
	var bs uint64 = 1024 * 1024
	buf := make([]byte, bs)

	for i := 0; i < len(buf); i++ {
		buf[i] = byte(i)
	}

	for i := uint64(0); i < size; i += bs {
		if size < bs {
			bs = size
		}
		fp.Write(buf[:bs])

	}
}

func testTempFile(t *testing.T, size uint64) (string, func()) {
	fp, err := ioutil.TempFile(".", testPrefix)
	if err != nil {
		t.Fatal(err)
	}
	defer fp.Close()

	if size > 0 {
		testFill(t, fp, size)
	}
	name := fp.Name()
	return name, func() {
		err := os.Remove(name)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func testAction(t *testing.T) string {
	return "action"
}
