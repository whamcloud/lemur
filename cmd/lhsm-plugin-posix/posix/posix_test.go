package posix_test

import "testing"

func TestArchive(t *testing.T) {
	defer testChdirTemp(t)()
	tfile, cleanFile := testTempFile(t, 1000)
	defer cleanFile()

	t.Logf("test file %s", tfile)
}
