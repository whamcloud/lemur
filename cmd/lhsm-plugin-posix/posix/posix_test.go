package posix_test

import (
	"bytes"
	"math"
	"os"
	"testing"

	"github.com/pborman/uuid"
	"github.com/pkg/errors"

	"github.intel.com/hpdd/lemur/cmd/lhsm-plugin-posix/posix"
	"github.intel.com/hpdd/lemur/dmplugin"
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
	tfile, cleanFile := testTempFile(t, 0)
	defer cleanFile()
	action := dmplugin.NewTestAction(t, tfile, offset, length, fileID, data)
	if err := mover.Restore(action); err != nil {
		t.Fatal(err)
	}
	return action
}

func testRestoreFail(t *testing.T, mover *posix.Mover, offset uint64, length uint64, fileID []byte, data []byte, outer error) *dmplugin.TestAction {
	tfile, cleanFile := testTempFile(t, 0)
	defer cleanFile()
	action := dmplugin.NewTestAction(t, tfile, offset, length, fileID, data)
	if err := mover.Restore(action); err == nil {
		var buf bytes.Buffer
		errors.Fprint(&buf, outer)
		t.Fatalf("expected restore failure at: %s", buf.String())
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

func TestArchive(t *testing.T) {
	WithPosixMover(t, nil, func(t *testing.T, mover *posix.Mover) {
		// trigger two updates (at current interval of 10MB
		var length uint64 = 20 * 1024 * 1024
		tfile, cleanFile := testTempFile(t, length)
		defer cleanFile()

		action := testArchive(t, mover, tfile, 0, length, nil, nil)

		if action.Updates != 2 {
			t.Fatalf("expected 2 updates, got %d", action.Updates)
		}

		testRestore(t, mover, 0, length, action.FileID(), nil)
	})
}

func TestArchiveMaxSize(t *testing.T) {
	WithPosixMover(t, nil, func(t *testing.T, mover *posix.Mover) {
		var length uint64 = 1000000
		tfile, cleanFile := testTempFile(t, length)
		defer cleanFile()

		// we received maxuint64 from coordinator, so test this as well
		action := testArchive(t, mover, tfile, 0, math.MaxUint64, nil, nil)
		testRestore(t, mover, 0, math.MaxUint64, action.FileID(), nil)
	})
}

func TestArchiveDefaultChecksum(t *testing.T) {
	defaultChecksum := func(cfg *posix.ChecksumConfig) *posix.ChecksumConfig {
		return cfg.Merge(nil)
	}
	WithPosixMover(t, defaultChecksum, func(t *testing.T, mover *posix.Mover) {
		var length uint64 = 100
		tfile, cleanFile := testTempFile(t, length)
		defer cleanFile()

		action := testArchive(t, mover, tfile, 0, length, nil, nil)
		testRestore(t, mover, 0, length, action.FileID(), nil)
	})
}

func TestArchiveRestoreBrokenFileID(t *testing.T) {
	defaultChecksum := func(cfg *posix.ChecksumConfig) *posix.ChecksumConfig {
		return cfg.Merge(nil)
	}
	WithPosixMover(t, defaultChecksum, func(t *testing.T, mover *posix.Mover) {
		var length uint64 = 100
		tfile, cleanFile := testTempFile(t, length)
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

func TestArchiveRestoreError(t *testing.T) {
	defaultChecksum := func(cfg *posix.ChecksumConfig) *posix.ChecksumConfig {
		return cfg.Merge(nil)
	}
	WithPosixMover(t, defaultChecksum, func(t *testing.T, mover *posix.Mover) {
		var length uint64 = 100
		tfile, cleanFile := testTempFile(t, length)
		defer cleanFile()

		// we received maxuint64 from coordinator, so test this as well
		action := testArchive(t, mover, tfile, 0, length, nil, nil)

		failRestore := func(t *testing.T, mover *posix.Mover, offset uint64, length uint64, fileID []byte, data []byte) *dmplugin.TestAction {
			tfile, cleanFile := testTempFile(t, 0)
			defer cleanFile()
			os.Chmod(tfile, 0444)
			action := dmplugin.NewTestAction(t, tfile, offset, length, fileID, data)
			if err := mover.Restore(action); err != nil {
				if !os.IsPermission(errors.Cause(err)) {
					t.Fatalf("Unexpected failure: %v", err)
				}
			} else {
				t.Fatal("Expected failure")
			}

			return action
		}

		failRestore(t, mover, 0, length, action.FileID(), nil)
	})
}

func TestArchiveNoChecksum(t *testing.T) {
	disableChecksum := func(cfg *posix.ChecksumConfig) *posix.ChecksumConfig {
		return cfg.Merge(&posix.ChecksumConfig{Disabled: true})
	}

	WithPosixMover(t, disableChecksum, func(t *testing.T, mover *posix.Mover) {
		var length uint64 = 1000000
		tfile, cleanFile := testTempFile(t, length)
		defer cleanFile()

		action := testArchive(t, mover, tfile, 0, math.MaxUint64, nil, nil)
		// we received maxuint64 from coordinator, so test this as well

		fileID, err := posix.ParseFileID(action.FileID())
		if err != nil {
			t.Fatal(err)
		}

		testCorruptFile(t, mover.Destination(fileID.UUID))

		// Successfully restore corrupt data
		testRestore(t, mover, 0, math.MaxUint64, action.FileID(), nil)
	})
}

func TestArchiveNoChecksumRestore(t *testing.T) {
	disableChecksum := func(cfg *posix.ChecksumConfig) *posix.ChecksumConfig {
		return cfg.Merge(&posix.ChecksumConfig{DisableCompareOnRestore: true})
	}

	WithPosixMover(t, disableChecksum, func(t *testing.T, mover *posix.Mover) {
		var length uint64 = 1000000
		tfile, cleanFile := testTempFile(t, length)
		defer cleanFile()

		action := testArchive(t, mover, tfile, 0, math.MaxUint64, nil, nil)
		// we received maxuint64 from coordinator, so test this as well

		fileID, err := posix.ParseFileID(action.FileID())
		if err != nil {
			t.Fatal(err)
		}

		testCorruptFile(t, mover.Destination(fileID.UUID))
		// Successfully restore corrupt data
		testRestore(t, mover, 0, math.MaxUint64, action.FileID(), nil)
	})
}

func TestArchiveChecksumAfter(t *testing.T) {
	WithPosixMover(t, nil, func(t *testing.T, mover *posix.Mover) {
		var length uint64 = 1000000
		tfile, cleanFile := testTempFile(t, length)
		defer cleanFile()

		// we received maxuint64 from coordinator, so test this as well
		action := testArchive(t, mover, tfile, 0, math.MaxUint64, nil, nil)
		// Disable checksum generation but should still check existing checksums
		mover.ChecksumConfig().Disabled = true
		testCorruptFile(t, testDestinationFile(t, mover, action.FileID()))
		// Don't  restore corrupt data
		testRestoreFail(t, mover, 0, math.MaxUint64, action.FileID(), nil, errors.New(""))
	})
}

func TestCorruptArchive(t *testing.T) {
	WithPosixMover(t, nil, func(t *testing.T, mover *posix.Mover) {
		var length uint64 = 1000000
		tfile, cleanFile := testTempFile(t, length)
		defer cleanFile()

		action := dmplugin.NewTestAction(t, tfile, 0, length, nil, nil)
		if err := mover.Archive(action); err != nil {
			t.Fatal(err)
		}

		path := testDestinationFile(t, mover, action.FileID())

		testCorruptFile(t, path)

		// TODO check for specific CheckSum error
		testRestoreFail(t, mover, 0, length, action.FileID(), nil, errors.New(""))

	})
}

func TestRemove(t *testing.T) {
	WithPosixMover(t, nil, func(t *testing.T, mover *posix.Mover) {
		var length uint64 = 1000000
		tfile, cleanFile := testTempFile(t, length)
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

	defer testChdirTemp(t)()
	archiveDir, cleanArchive := testTempDir(t)
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
