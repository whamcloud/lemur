package posix_test

import (
	"testing"

	"github.intel.com/hpdd/policy/pdm/dmplugin"
	"github.intel.com/hpdd/policy/pdm/lhsm-plugin-posix/posix"
	"github.intel.com/hpdd/policy/pkg/client"
)

func TestArchive(t *testing.T) {
	length := 1000
	tdir, cleanDir := testChdirTemp(t)
	defer cleanDir()
	tfile, cleanFile := testTempFile(t, length)
	defer cleanFile()
	c := client.Test(tdir)
	archiveDir, cleanArchive := testTempDir(t)
	defer cleanArchive()

	mover, err := posix.NewMover(&posix.MoverConfig{
		Name:       "posix-test",
		Client:     c,
		ArchiveDir: archiveDir,
	})
	if err != nil {
		t.Fatal(err)
	}

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
}
