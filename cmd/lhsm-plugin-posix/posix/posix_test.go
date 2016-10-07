// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package posix_test

import (
	"math"
	"os"
	"testing"

	"github.com/pborman/uuid"
	"github.com/pkg/errors"

	"github.com/intel-hpdd/lemur/cmd/lhsm-plugin-posix/posix"
	"github.com/intel-hpdd/lemur/dmplugin"
	"github.com/intel-hpdd/lemur/internal/testhelpers"
)

func testArchive(t *testing.T, mover *posix.Mover, path string, offset uint64, length uint64, fileID []byte, data []byte) *dmplugin.TestAction {
	action := dmplugin.NewTestAction(t, path, offset, length, fileID, data)
	if err := mover.Archive(action); err != nil {
		t.Fatal(err)
	}
	return action
}

func testRemove(t *testing.T, mover *posix.Mover, fileID []byte, data []byte) *dmplugin.TestAction {
	action := dmplugin.NewTestAction(t, "", 0, 0, fileID, data)
	if err := mover.Remove(action); err != nil {
		t.Fatal(err)
	}
	return action
}

func testRestore(t *testing.T, mover *posix.Mover, offset uint64, length uint64, fileID []byte, data []byte) *dmplugin.TestAction {
	tfile, cleanFile := testhelpers.TempFile(t, 0)
	defer cleanFile()
	action := dmplugin.NewTestAction(t, tfile, offset, length, fileID, data)
	if err := mover.Restore(action); err != nil {
		t.Fatal(err)
	}
	return action
}

func testRestoreFail(t *testing.T, mover *posix.Mover, offset uint64, length uint64, fileID []byte, data []byte, outer error) *dmplugin.TestAction {
	tfile, cleanFile := testhelpers.TempFile(t, 0)
	defer cleanFile()
	action := dmplugin.NewTestAction(t, tfile, offset, length, fileID, data)
	if err := mover.Restore(action); err == nil {
		t.Fatalf("expected restore failure at: %s", outer)
	} else {
		t.Logf("got expected error: %v", err)
	}
	return action
}

func testDestinationFile(t *testing.T, mover *posix.Mover, buf []byte) string {
	fileID, err := posix.ParseFileID(buf)
	if err != nil {
		t.Fatal(err)
	}

	return mover.Destination(fileID.UUID)
}

func TestPosixArchive(t *testing.T) {
	WithPosixMover(t, nil, func(t *testing.T, mover *posix.Mover) {
		// trigger two updates (at current interval of 10MB
		var length uint64 = 20 * 1024 * 1024
		tfile, cleanFile := testhelpers.TempFile(t, length)
		defer cleanFile()

		action := testArchive(t, mover, tfile, 0, length, nil, nil)

		// Need to introduce a delay to  test new time based updates.
		if action.Updates != 0 {
			t.Fatalf("expected 0 updates, got %d", action.Updates)
		}

		testRestore(t, mover, 0, length, action.FileID(), nil)
	})
}

func TestPosixArchiveMaxSize(t *testing.T) {
	WithPosixMover(t, nil, func(t *testing.T, mover *posix.Mover) {
		var length uint64 = 1000000
		tfile, cleanFile := testhelpers.TempFile(t, length)
		defer cleanFile()

		// we received maxuint64 from coordinator, so test this as well
		action := testArchive(t, mover, tfile, 0, math.MaxUint64, nil, nil)
		testRestore(t, mover, 0, math.MaxUint64, action.FileID(), nil)
	})
}

func TestPosixArchiveDefaultChecksum(t *testing.T) {
	defaultChecksum := func(cfg *posix.ChecksumConfig) *posix.ChecksumConfig {
		return cfg.Merge(nil)
	}
	WithPosixMover(t, defaultChecksum, func(t *testing.T, mover *posix.Mover) {
		var length uint64 = 100
		tfile, cleanFile := testhelpers.TempFile(t, length)
		defer cleanFile()

		action := testArchive(t, mover, tfile, 0, length, nil, nil)
		testRestore(t, mover, 0, length, action.FileID(), nil)
	})
}

func TestPosixArchiveRestoreBrokenFileID(t *testing.T) {
	defaultChecksum := func(cfg *posix.ChecksumConfig) *posix.ChecksumConfig {
		return cfg.Merge(nil)
	}
	WithPosixMover(t, defaultChecksum, func(t *testing.T, mover *posix.Mover) {
		var length uint64 = 100
		tfile, cleanFile := testhelpers.TempFile(t, length)
		defer cleanFile()

		action := testArchive(t, mover, tfile, 0, length, nil, nil)
		fileID, err := posix.ParseFileID(action.FileID())
		if err != nil {
			t.Fatal(err)
		}
		fileID.UUID = uuid.New()
		buf, err := posix.EncodeFileID(fileID)
		if err != nil {
			t.Fatal(err)
		}

		// Wrong UUID
		action.SetFileID(buf)
		testRestoreFail(t, mover, 0, length, action.FileID(), nil, errors.New(""))

		// Missing FileID
		action.SetFileID(nil)
		testRestoreFail(t, mover, 0, length, action.FileID(), nil, errors.New(""))

		// Garbage FildID
		action.SetFileID([]byte(`{"Not a FileID"}`))
		testRestoreFail(t, mover, 0, length, action.FileID(), nil, errors.New(""))
	})
}

func TestPosixArchiveRestoreError(t *testing.T) {
	defaultChecksum := func(cfg *posix.ChecksumConfig) *posix.ChecksumConfig {
		return cfg.Merge(nil)
	}
	WithPosixMover(t, defaultChecksum, func(t *testing.T, mover *posix.Mover) {
		var length uint64 = 100
		tfile, cleanFile := testhelpers.TempFile(t, length)
		defer cleanFile()

		// we received maxuint64 from coordinator, so test this as well
		action := testArchive(t, mover, tfile, 0, length, nil, nil)

		failRestore := func(t *testing.T, mover *posix.Mover, offset uint64, length uint64, fileID []byte, data []byte) *dmplugin.TestAction {
			tfile, cleanFile := testhelpers.TempFile(t, 0)
			defer cleanFile()
			os.Chmod(tfile, 0444)
			action := dmplugin.NewTestAction(t, tfile, offset, length, fileID, data)
			if err := mover.Restore(action); err != nil {
				if !os.IsPermission(errors.Cause(err)) {
					t.Fatalf("Unexpected failure: %v", err)
				}
			} else {
				fi, _ := os.Stat(tfile)
				t.Fatalf("Expected permission failure: %s mode:0%o", fi.Name(), fi.Mode())
			}

			return action
		}

		failRestore(t, mover, 0, length, action.FileID(), nil)
	})
}

func TestPosixArchiveNoChecksum(t *testing.T) {
	disableChecksum := func(cfg *posix.ChecksumConfig) *posix.ChecksumConfig {
		return cfg.Merge(&posix.ChecksumConfig{Disabled: true})
	}

	WithPosixMover(t, disableChecksum, func(t *testing.T, mover *posix.Mover) {
		var length uint64 = 1000000
		tfile, cleanFile := testhelpers.TempFile(t, length)
		defer cleanFile()

		action := testArchive(t, mover, tfile, 0, math.MaxUint64, nil, nil)
		// we received maxuint64 from coordinator, so test this as well

		fileID, err := posix.ParseFileID(action.FileID())
		if err != nil {
			t.Fatal(err)
		}

		testhelpers.CorruptFile(t, mover.Destination(fileID.UUID))

		// Successfully restore corrupt data
		testRestore(t, mover, 0, math.MaxUint64, action.FileID(), nil)
	})
}

func TestPosixArchiveNoChecksumRestore(t *testing.T) {
	disableChecksum := func(cfg *posix.ChecksumConfig) *posix.ChecksumConfig {
		return cfg.Merge(&posix.ChecksumConfig{DisableCompareOnRestore: true})
	}

	WithPosixMover(t, disableChecksum, func(t *testing.T, mover *posix.Mover) {
		var length uint64 = 1000000
		tfile, cleanFile := testhelpers.TempFile(t, length)
		defer cleanFile()

		action := testArchive(t, mover, tfile, 0, math.MaxUint64, nil, nil)
		// we received maxuint64 from coordinator, so test this as well

		fileID, err := posix.ParseFileID(action.FileID())
		if err != nil {
			t.Fatal(err)
		}

		testhelpers.CorruptFile(t, mover.Destination(fileID.UUID))
		// Successfully restore corrupt data
		testRestore(t, mover, 0, math.MaxUint64, action.FileID(), nil)
	})
}

func TestPosixArchiveChecksumAfter(t *testing.T) {
	WithPosixMover(t, nil, func(t *testing.T, mover *posix.Mover) {
		var length uint64 = 1000000
		tfile, cleanFile := testhelpers.TempFile(t, length)
		defer cleanFile()

		// we received maxuint64 from coordinator, so test this as well
		action := testArchive(t, mover, tfile, 0, math.MaxUint64, nil, nil)
		// Disable checksum generation but should still check existing checksums
		mover.ChecksumConfig().Disabled = true
		testhelpers.CorruptFile(t, testDestinationFile(t, mover, action.FileID()))
		// Don't  restore corrupt data
		testRestoreFail(t, mover, 0, math.MaxUint64, action.FileID(), nil, errors.New(""))
	})
}

func TestPosixCorruptArchive(t *testing.T) {
	WithPosixMover(t, nil, func(t *testing.T, mover *posix.Mover) {
		var length uint64 = 1000000
		tfile, cleanFile := testhelpers.TempFile(t, length)
		defer cleanFile()

		action := dmplugin.NewTestAction(t, tfile, 0, length, nil, nil)
		if err := mover.Archive(action); err != nil {
			t.Fatal(err)
		}

		path := testDestinationFile(t, mover, action.FileID())

		testhelpers.CorruptFile(t, path)

		// TODO check for specific CheckSum error
		testRestoreFail(t, mover, 0, length, action.FileID(), nil, errors.New(""))

	})
}

func TestPosixRemove(t *testing.T) {
	WithPosixMover(t, nil, func(t *testing.T, mover *posix.Mover) {
		var length uint64 = 1000000
		tfile, cleanFile := testhelpers.TempFile(t, length)
		defer cleanFile()

		action := testArchive(t, mover, tfile, 0, length, nil, nil)
		path := testDestinationFile(t, mover, action.FileID())

		if _, err := os.Stat(path); err != nil {
			t.Fatalf("Destination file is missing: %v", err)
		}

		testRemove(t, mover, action.FileID(), nil)

		_, err := os.Stat(path)
		if !os.IsNotExist(err) {
			t.Fatalf("Unexpected or missing error: %v", err)
		}

		testRestoreFail(t, mover, 0, length, action.FileID(), nil, errors.New(""))
	})
}

func WithPosixMover(t *testing.T, updateConfig func(*posix.ChecksumConfig) *posix.ChecksumConfig,
	tester func(t *testing.T, mover *posix.Mover)) {

	defer testhelpers.ChdirTemp(t)()
	archiveDir, cleanArchive := testhelpers.TempDir(t)
	defer cleanArchive()

	var config *posix.ChecksumConfig
	if updateConfig != nil {
		config = updateConfig(nil)
	}

	mover, err := posix.NewMover("posix-test", archiveDir, config)
	if err != nil {
		t.Fatal(err)
	}

	tester(t, mover)
}
