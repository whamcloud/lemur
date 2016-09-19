package testhelpers

import (
	"io/ioutil"
	"os"
	"testing"
)

var testPrefix = "ptest"

func TempDir(t *testing.T) (string, func()) {
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

func ChdirTemp(t *testing.T) func() {
	tdir, cleanDir := TempDir(t)

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

func Fill(t *testing.T, fp *os.File, size uint64) {
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

func CorruptFile(t *testing.T, path string) {
	fp, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	_, err = fp.Write([]byte("Silent data corruption. :)"))
	if err != nil {
		t.Fatal(err)

	}
	err = fp.Close()
	if err != nil {
		t.Fatal(err)

	}
}

func TempFile(t *testing.T, size uint64) (string, func()) {
	fp, err := ioutil.TempFile(".", testPrefix)
	if err != nil {
		t.Fatal(err)
	}
	defer fp.Close()

	if size > 0 {
		Fill(t, fp, size)
	}
	name := fp.Name()
	return name, func() {
		err := os.Remove(name)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func CopyFile(t *testing.T, src string, dest string, mode os.FileMode) {
	buf, err := ioutil.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}

	err = ioutil.WriteFile(dest, buf, mode)
	if err != nil {
		t.Fatal(err)
	}
}

func TempCopy(t *testing.T, src string, mode os.FileMode) (string, func()) {
	tmpFile, cleanup := TempFile(t, 0)
	CopyFile(t, src, tmpFile, mode)

	/* ensure file has correct mode, in case we're overwriting */
	err := os.Chmod(tmpFile, mode)
	if err != nil {
		t.Fatal(err)
	}

	return tmpFile, cleanup
}

func Action(t *testing.T) string {
	return "action"
}
