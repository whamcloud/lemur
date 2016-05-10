package posix_test

import (
	"os"
	"testing"

	"github.intel.com/hpdd/policy/pdm/dmplugin"
	"github.intel.com/hpdd/policy/pdm/lhsm-plugin-posix/posix"
)

func TestArchive(t *testing.T) {
	WithPosixMover(t, func(t *testing.T, mover *posix.Mover) {
		length := 1000000
		tfile, cleanFile := testTempFile(t, length)
		defer cleanFile()

		action := dmplugin.NewTestAction(t, tfile, 0, int64(length), nil, nil)
		if err := mover.Archive(action); err != nil {
			t.Fatal(err)
		}

		newFile, cleanFile2 := testTempFile(t, 0)
		defer cleanFile2()

		restore := dmplugin.NewTestAction(t, newFile, 0, int64(length), []byte(action.FileID()), nil)
		if err := mover.Restore(restore); err != nil {
			t.Fatal(err)
		}
	})
}

func TestCorruptArchive(t *testing.T) {
	WithPosixMover(t, func(t *testing.T, mover *posix.Mover) {
		length := 1000000
		tfile, cleanFile := testTempFile(t, length)
		defer cleanFile()

		action := dmplugin.NewTestAction(t, tfile, 0, int64(length), nil, nil)
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

		restore := dmplugin.NewTestAction(t, newFile, 0, int64(length), []byte(action.FileID()), nil)
		err = mover.Restore(restore)
		if err == nil {
			t.Fatal("Data corruption not detected")
		}
		// TODO check for specific CheckSum error

	})
}

func TestRemove(t *testing.T) {
	WithPosixMover(t, func(t *testing.T, mover *posix.Mover) {
		length := 1000000
		tfile, cleanFile := testTempFile(t, length)
		defer cleanFile()

		action := dmplugin.NewTestAction(t, tfile, 0, int64(length), nil, nil)
		if err := mover.Archive(action); err != nil {
			t.Fatal(err)
		}

		remove := dmplugin.NewTestAction(t, "", 0, 0, []byte(action.FileID()), nil)
		if err := mover.Remove(remove); err != nil {
			t.Fatal(err)
		}

		newFile, cleanFile2 := testTempFile(t, 0)
		defer cleanFile2()

		restore := dmplugin.NewTestAction(t, newFile, 0, int64(length), []byte(action.FileID()), nil)
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
