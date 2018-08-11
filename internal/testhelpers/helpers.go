// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package testhelpers

import (
	"io"
	"io/ioutil"
	"os"
	"testing"
)

var testPrefix = "ptest"

// TempDir returns path to a new temporary directory and function that will
// forcibly remove it.
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

// ChdirTemp changes the working directory to a new TempDir. The cleanup
// function returns to the previous working directoy and removes the temp
// directory.
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

// Fill writes size amount of bytes to the file.
func Fill(t *testing.T, fp io.Writer, size int64) {
	var bs int64 = 1024 * 1024
	buf := make([]byte, bs)

	for i := 0; i < len(buf); i++ {
		buf[i] = byte(i)
	}

	for i := int64(0); i < size; i += bs {
		if size < bs {
			bs = size
		}
		fp.Write(buf[:bs])

	}
}

// CorruptFile writes an string to the beginning of the file.
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

// TempFile creates a temporary file. If size is >0 then that amount of bytes
// will be written to the file.
func TempFile(t *testing.T, size int64) (string, func()) {
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

// CopyFile copies data from one file another. If the target file does  not
// exist then it will be created with the given mode. This is a non-optimal copy
// and not intended to be used for very large files.
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

// TempCopy copies provided file to a new temp file that will be assigned the
// provided mode after the copy. (So the mode can specify a read-only file.)
func TempCopy(t *testing.T, src string, mode os.FileMode) (string, func()) {
	tmpFile, cleanup := TempFile(t, 0)
	CopyFile(t, src, tmpFile, mode)

	// ensure file has correct mode, in case we're overwriting
	err := os.Chmod(tmpFile, mode)
	if err != nil {
		t.Fatal(err)
	}

	return tmpFile, cleanup
}

// Action yields "action".
// Srsly, wtf?
func Action(t *testing.T) string {
	return "action"
}
