// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"os"
	"testing"
	"time"

	lustre "github.com/intel-hpdd/go-lustre"
	"github.com/intel-hpdd/lemur/dmplugin"
	"github.com/intel-hpdd/lemur/internal/testhelpers"
	"github.com/intel-hpdd/lemur/pkg/checksum"
	"github.com/intel-hpdd/logging/debug"
)

func testArchive(t *testing.T, mover *Mover, path string, offset int64, length int64, fileID string, data []byte) *dmplugin.TestAction {
	action := dmplugin.NewTestAction(t, path, offset, length, fileID, data)
	if err := mover.Archive(action); err != nil {
		t.Fatal(err)
	}
	return action
}

func testRemove(t *testing.T, mover *Mover, fileID string, data []byte) *dmplugin.TestAction {
	action := dmplugin.NewTestAction(t, "", 0, 0, fileID, data)
	if err := mover.Remove(action); err != nil {
		t.Fatal(err)
	}
	return action
}

func testRestore(t *testing.T, mover *Mover, offset int64, length int64, fileID string, data []byte) *dmplugin.TestAction {
	tfile, cleanFile := testhelpers.TempFile(t, 0)
	defer cleanFile()
	action := dmplugin.NewTestAction(t, tfile, offset, length, fileID, data)
	if err := mover.Restore(action); err != nil {
		t.Fatal(err)
	}
	return action
}

func testRestoreFail(t *testing.T, mover *Mover, offset int64, length int64, fileID string, data []byte) *dmplugin.TestAction {
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

func TestS3Extents(t *testing.T) {
	WithS3Mover(t, nil, func(t *testing.T, mover *Mover) {
		type extent struct {
			id     string
			offset int64
			length int64
		}
		var extents []extent
		var maxExtent int64 = 1024 * 1024
		var fileSize int64 = 4*1024*1024 + 42
		tfile, cleanFile := testhelpers.TempFile(t, fileSize)
		defer cleanFile()

		st, err := os.Stat(tfile)
		if err != nil {
			t.Fatal(err)
		}
		actualSize := st.Size()
		startSum, err := checksum.FileSha1Sum(tfile)
		if err != nil {
			t.Fatal(err)
		}
		debug.Printf("%s actual size: %d", tfile, actualSize)

		for offset := int64(0); offset < actualSize; offset += maxExtent {
			length := maxExtent
			if offset+maxExtent > actualSize {
				length = actualSize - offset
			}
			aa := dmplugin.NewTestAction(t, tfile, offset, length, "", nil)
			if err := mover.Archive(aa); err != nil {
				t.Fatal(err)
			}
			extents = append(extents, extent{aa.UUID(), offset, length})

			debug.Printf("ARCHIVE %d/%d/%d: %s", offset, offset+length, actualSize, aa.UUID())
		}

		// Zap the test file like it was released before restoring
		// the data.
		if err := os.Truncate(tfile, 0); err != nil {
			t.Fatal(err)
		}
		for _, extent := range extents {
			ra := dmplugin.NewTestAction(t, tfile, extent.offset, extent.length, extent.id, nil)

			if err := mover.Restore(ra); err != nil {
				t.Fatal(err)
			}

			debug.Printf("RESTORE %d/%d/%d: %s", extent.offset, extent.offset+extent.length, actualSize, ra.UUID())
		}

		endSum, err := checksum.FileSha1Sum(tfile)
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(endSum, startSum) {
			t.Fatalf("end sum (%x) != start sum (%x)", endSum, startSum)
		}
	})
}

func TestS3Archive(t *testing.T) {
	// Quick hack to let us test against radosgw; a better solution
	// would be to figure out some kind of fallback.
	embiggenUploadPartSize := func(cfg *archiveConfig) *archiveConfig {
		cfg.UploadPartSize = 128 * 1024 * 1024
		return cfg
	}
	if os.Getenv("LHSM_TEST_RADOSGW") == "" {
		embiggenUploadPartSize = nil
	}

	WithS3Mover(t, embiggenUploadPartSize, func(t *testing.T, mover *Mover) {
		// trigger two updates (at current interval of 10MB
		var length int64 = 20 * 1024 * 1024
		tfile, cleanFile := testhelpers.TempFile(t, length)
		defer cleanFile()

		start := time.Now()
		action := testArchive(t, mover, tfile, 0, length, "", nil)

		// TODO: parameterize the update interval
		expectedUpdates := int((time.Since(start) / time.Second) / 10)

		if action.Updates != expectedUpdates {
			t.Errorf("expected %d updates, got %d", expectedUpdates, action.Updates)
		}

		start = time.Now()
		restore := testRestore(t, mover, 0, length, action.UUID(), nil)
		// TODO: parameterize the update interval
		duration := time.Since(start)
		expectedUpdates = int((duration / time.Second) / 10)
		if restore.Updates != expectedUpdates {
			t.Errorf("expected %d updates, got %d, duration: %v", expectedUpdates, restore.Updates, duration)
		}
		testRemove(t, mover, action.UUID(), nil)
	})
}

func TestS3ArchiveMaxSize(t *testing.T) {
	WithS3Mover(t, nil, func(t *testing.T, mover *Mover) {
		var length int64 = 1000000
		tfile, cleanFile := testhelpers.TempFile(t, length)
		defer cleanFile()

		// we received MaxExtentLength from coordinator, so test this as well
		action := testArchive(t, mover, tfile, 0, lustre.MaxExtentLength, "", nil)
		testRestore(t, mover, 0, lustre.MaxExtentLength, action.UUID(), nil)
		testRemove(t, mover, action.UUID(), nil)
	})
}

/*
func TestS3ArchiveNoChecksum(t *testing.T) {
	disableChecksum := func(cfg *MoverConfig) *MoverConfig {
		cfg.Checksums.Disabled = true
		return cfg
	}

	WithS3Mover(t, disableChecksum, func(t *testing.T, mover *Mover) {
		var length int64 = 1000000
		tfile, cleanFile := testTempFile(t, length)
		defer cleanFile()

		action := testArchive(t, mover, tfile, 0, lustre.MaxExtentLength, nil, nil)
		// we received MaxExtentLength from coordinator, so test this as well

		fileID, err := posix.ParseFileID(action.FileID())
		if err != nil {
			t.Fatal(err)
		}

		testCorruptFile(t, mover.Destination(fileID.UUID))

		// Successfully restore corrupt data
		testRestore(t, mover, 0, lustre.MaxExtentLength, action.FileID(), nil)
	})
}

func TestS3ArchiveNoChecksumRestore(t *testing.T) {
	disableChecksum := func(cfg *MoverConfig) *MoverConfig {
		cfg.Checksums.DisableCompareOnRestore = true
		return cfg
	}

	WithS3Mover(t, disableChecksum, func(t *testing.T, mover *Mover) {
		var length int64 = 1000000
		tfile, cleanFile := testTempFile(t, length)
		defer cleanFile()

		action := testArchive(t, mover, tfile, 0, lustre.MaxExtentLength, nil, nil)
		// we received MaxExtentLength from coordinator, so test this as well

		fileID, err := posix.ParseFileID(action.FileID())
		if err != nil {
			t.Fatal(err)
		}

		testCorruptFile(t, mover.Destination(fileID.UUID))
		// Successfully restore corrupt data
		testRestore(t, mover, 0, lustre.MaxExtentLength, action.FileID(), nil)
	})
}

func TestS3ArchiveChecksumAfter(t *testing.T) {
	WithS3Mover(t, nil, func(t *testing.T, mover *Mover) {
		var length int64 = 1000000
		tfile, cleanFile := testTempFile(t, length)
		defer cleanFile()

		// we received MaxExtentLength from coordinator, so test this as well
		action := testArchive(t, mover, tfile, 0, lustre.MaxExtentLength, nil, nil)
		// Disable checksum generation but should still check existing checksums
		mover.ChecksumConfig().Disabled = true
		testCorruptFile(t, testDestinationFile(t, mover, action.FileID()))
		// Don't  restore corrupt data
		testRestoreFail(t, mover, 0, lustre.MaxExtentLength, action.FileID(), nil)
	})
}
*/

/*
func TestS3CorruptArchive(t *testing.T) {
	WithS3Mover(t, nil, func(t *testing.T, mover *Mover) {
		var length int64 = 1000000
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
func TestS3Remove(t *testing.T) {
	WithS3Mover(t, nil, func(t *testing.T, mover *Mover) {
		var length int64 = 1000000
		tfile, cleanFile := testhelpers.TempFile(t, length)
		defer cleanFile()

		action := testArchive(t, mover, tfile, 0, length, "", nil)

		testRemove(t, mover, action.UUID(), nil)
		testRestoreFail(t, mover, 0, length, action.UUID(), nil)
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

	bucket := os.Getenv(bucketVar)
	if bucket == "" {
		t.Skipf("Set %q in environment to test S3 mover.", bucketVar)
	}

	config := &archiveConfig{
		Name:     "test-s3",
		Region:   region,
		Bucket:   bucket,
		Prefix:   "ptest",
		Endpoint: os.Getenv("AWS_S3_ENDPOINT"),
	}

	if updateConfig != nil {
		config = updateConfig(config)
	}

	defer testhelpers.ChdirTemp(t)()
	mover := S3Mover(config, s3Svc(config), 1)

	tester(t, mover)
}
