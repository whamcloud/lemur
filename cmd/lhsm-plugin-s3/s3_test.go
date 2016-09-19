package main

import (
	"math"
	"os"
	"testing"
	"time"

	"github.intel.com/hpdd/lemur/dmplugin"
	"github.intel.com/hpdd/lemur/internal/testhelpers"
)

func testArchive(t *testing.T, mover *Mover, path string, offset uint64, length uint64, fileID []byte, data []byte) *dmplugin.TestAction {
	action := dmplugin.NewTestAction(t, path, offset, length, fileID, data)
	if err := mover.Archive(action); err != nil {
		t.Fatal(err)
	}
	return action
}

func testRemove(t *testing.T, mover *Mover, fileID []byte, data []byte) *dmplugin.TestAction {
	action := dmplugin.NewTestAction(t, "", 0, 0, fileID, data)
	if err := mover.Remove(action); err != nil {
		t.Fatal(err)
	}
	return action
}

func testRestore(t *testing.T, mover *Mover, offset uint64, length uint64, fileID []byte, data []byte) *dmplugin.TestAction {
	tfile, cleanFile := testhelpers.TempFile(t, 0)
	defer cleanFile()
	action := dmplugin.NewTestAction(t, tfile, offset, length, fileID, data)
	if err := mover.Restore(action); err != nil {
		t.Fatal(err)
	}
	return action
}

func testRestoreFail(t *testing.T, mover *Mover, offset uint64, length uint64, fileID []byte, data []byte) *dmplugin.TestAction {
	tfile, cleanFile := testhelpers.TempFile(t, 0)
	defer cleanFile()
	action := dmplugin.NewTestAction(t, tfile, offset, length, fileID, data)
	if err := mover.Restore(action); err == nil {
		t.Fatal("Expected restore to fail")
	}
	return action
}

/*
func testDestinationFile(t *testing.T, mover *Mover, buf []byte) string {
	fileID, err := posix.ParseFileID(buf)
	if err != nil {
		t.Fatal(err)
	}

	return mover.Destination(fileID.UUID)
}
*/

func TestArchive(t *testing.T) {
	WithS3Mover(t, nil, func(t *testing.T, mover *Mover) {
		// trigger two updates (at current interval of 10MB
		var length uint64 = 20 * 1024 * 1024
		tfile, cleanFile := testhelpers.TempFile(t, length)
		defer cleanFile()

		start := time.Now()
		action := testArchive(t, mover, tfile, 0, length, nil, nil)

		// TODO: parameterize the update interval
		expectedUpdates := int((time.Since(start) / time.Second) / 10)

		if action.Updates != expectedUpdates {
			t.Errorf("expected %d updates, got %d", expectedUpdates, action.Updates)
		}

		start = time.Now()
		restore := testRestore(t, mover, 0, length, action.FileID(), nil)
		// TODO: parameterize the update interval
		duration := time.Since(start)
		expectedUpdates = int((duration / time.Second) / 10)
		if restore.Updates != expectedUpdates {
			t.Errorf("expected %d updates, got %d, duration: %v", expectedUpdates, restore.Updates, duration)
		}
		testRemove(t, mover, action.FileID(), nil)
	})
}

func TestArchiveMaxSize(t *testing.T) {
	WithS3Mover(t, nil, func(t *testing.T, mover *Mover) {
		var length uint64 = 1000000
		tfile, cleanFile := testhelpers.TempFile(t, length)
		defer cleanFile()

		// we received maxuint64 from coordinator, so test this as well
		action := testArchive(t, mover, tfile, 0, math.MaxUint64, nil, nil)
		testRestore(t, mover, 0, math.MaxUint64, action.FileID(), nil)
		testRemove(t, mover, action.FileID(), nil)
	})
}

/*
func TestArchiveNoChecksum(t *testing.T) {
	disableChecksum := func(cfg *MoverConfig) *MoverConfig {
		cfg.Checksums.Disabled = true
		return cfg
	}

	WithS3Mover(t, disableChecksum, func(t *testing.T, mover *Mover) {
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
	disableChecksum := func(cfg *MoverConfig) *MoverConfig {
		cfg.Checksums.DisableCompareOnRestore = true
		return cfg
	}

	WithS3Mover(t, disableChecksum, func(t *testing.T, mover *Mover) {
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
	WithS3Mover(t, nil, func(t *testing.T, mover *Mover) {
		var length uint64 = 1000000
		tfile, cleanFile := testTempFile(t, length)
		defer cleanFile()

		// we received maxuint64 from coordinator, so test this as well
		action := testArchive(t, mover, tfile, 0, math.MaxUint64, nil, nil)
		// Disable checksum generation but should still check existing checksums
		mover.ChecksumConfig().Disabled = true
		testCorruptFile(t, testDestinationFile(t, mover, action.FileID()))
		// Don't  restore corrupt data
		testRestoreFail(t, mover, 0, math.MaxUint64, action.FileID(), nil)
	})
}
*/

/*
func TestCorruptArchive(t *testing.T) {
	WithS3Mover(t, nil, func(t *testing.T, mover *Mover) {
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
		testRestoreFail(t, mover, 0, length, action.FileID(), nil)

	})
}
*/
func TestRemove(t *testing.T) {
	WithS3Mover(t, nil, func(t *testing.T, mover *Mover) {
		var length uint64 = 1000000
		tfile, cleanFile := testhelpers.TempFile(t, length)
		defer cleanFile()

		action := testArchive(t, mover, tfile, 0, length, nil, nil)

		testRemove(t, mover, action.FileID(), nil)
		testRestoreFail(t, mover, 0, length, action.FileID(), nil)
	})
}

func WithS3Mover(t *testing.T, updateConfig func(*archiveConfig) *archiveConfig,
	tester func(t *testing.T, mover *Mover)) {
	bucketVar := "LHSM_TEST_BUCKET"
	// Default region to us-east-1
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1"
	}

	s3Endpoint := os.Getenv("AWS_S3_ENDPOINT")

	bucket := os.Getenv(bucketVar)
	if bucket == "" {
		t.Skipf("Set %q in environment to test S3 mover.", bucketVar)
	}

	config := &archiveConfig{
		Name:   "test-s3",
		Region: region,
		Bucket: bucket,
		Prefix: "ptest",
	}

	if updateConfig != nil {
		config = updateConfig(config)
	}

	defer testhelpers.ChdirTemp(t)()
	svc := s3Svc(config.Region, s3Endpoint)
	mover := S3Mover(svc, 1, config.Bucket, config.Prefix)

	tester(t, mover)
}
