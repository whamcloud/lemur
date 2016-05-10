package posix_test

import (
	"math"
	"os"
	"testing"

	"github.intel.com/hpdd/policy/pdm/dmplugin"
	"github.intel.com/hpdd/policy/pdm/lhsm-plugin-posix/posix"
)

func TestArchive(t *testing.T) {
	WithPosixMover(t, func(t *testing.T, mover *posix.Mover) {
		// trigger two updates (at current interval of 10MB
		var length uint64 = 20 * 1024 * 1024
		tfile, cleanFile := testTempFile(t, length)
		defer cleanFile()

		action := dmplugin.NewTestAction(t, tfile, 0, length, nil, nil)
		if err := mover.Archive(action); err != nil {
			t.Fatal(err)
		}
		if action.Updates != 2 {
			t.Fatalf("expected 2 updates, got %d", action.Updates)
		}

		newFile, cleanFile2 := testTempFile(t, 0)
		defer cleanFile2()

		restore := dmplugin.NewTestAction(t, newFile, 0, length, []byte(action.FileID()), nil)
		if err := mover.Restore(restore); err != nil {
			t.Fatal(err)
		}
	})
}

func TestArchiveMaxSize(t *testing.T) {
	WithPosixMover(t, func(t *testing.T, mover *posix.Mover) {
		var length uint64 = 1000000
		tfile, cleanFile := testTempFile(t, length)
		defer cleanFile()

		// we received maxuint64 from coordinator, so test this as well
		action := dmplugin.NewTestAction(t, tfile, 0, math.MaxUint64, nil, nil)
		if err := mover.Archive(action); err != nil {
			t.Fatal(err)
		}

		newFile, cleanFile2 := testTempFile(t, 0)
		defer cleanFile2()

		restore := dmplugin.NewTestAction(t, newFile, 0, math.MaxUint64, []byte(action.FileID()), nil)
		if err := mover.Restore(restore); err != nil {
			t.Fatal(err)
		}
	})
}

func TestCorruptArchive(t *testing.T) {
	WithPosixMover(t, func(t *testing.T, mover *posix.Mover) {
		var length uint64 = 1000000
		tfile, cleanFile := testTempFile(t, length)
		defer cleanFile()

		action := dmplugin.NewTestAction(t, tfile, 0, length, nil, nil)
		if err := mover.Archive(action); err != nil {
			t.Fatal(err)
		}

		fileID, err := posix.ParseFileID(action.FileID())
		if err != nil {
			t.Fatal(err)
		}
		fp, err := os.OpenFile(mover.Destination(fileID.UUID), os.O_RDWR, 0644)
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

		newFile, cleanFile2 := testTempFile(t, 0)
		defer cleanFile2()

		restore := dmplugin.NewTestAction(t, newFile, 0, length, []byte(action.FileID()), nil)
		err = mover.Restore(restore)
		if err == nil {
			t.Fatal("Data corruption not detected")
		}
		// TODO check for specific CheckSum error

	})
}

func TestRemove(t *testing.T) {
	WithPosixMover(t, func(t *testing.T, mover *posix.Mover) {
		var length uint64 = 1000000
		tfile, cleanFile := testTempFile(t, length)
		defer cleanFile()

		action := dmplugin.NewTestAction(t, tfile, 0, length, nil, nil)
		if err := mover.Archive(action); err != nil {
			t.Fatal(err)
		}

		remove := dmplugin.NewTestAction(t, "", 0, 0, []byte(action.FileID()), nil)
		if err := mover.Remove(remove); err != nil {
			t.Fatal(err)
		}

		newFile, cleanFile2 := testTempFile(t, 0)
		defer cleanFile2()

		restore := dmplugin.NewTestAction(t, newFile, 0, length, []byte(action.FileID()), nil)
		if err := mover.Restore(restore); err == nil {
			t.Fatal("Restore should have failed")
		}
	})
}

func WithPosixMover(t *testing.T, tester func(t *testing.T, mover *posix.Mover)) {
	defer testChdirTemp(t)()
	archiveDir, cleanArchive := testTempDir(t)
	defer cleanArchive()

	mover, err := posix.NewMover(&posix.MoverConfig{
		Name:       "posix-test",
		ArchiveDir: archiveDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	tester(t, mover)
}
