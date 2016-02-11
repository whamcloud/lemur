package main

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"time"

	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/policy/pdm/dmplugin"
	"github.intel.com/hpdd/policy/pkg/client"

	"code.google.com/p/go-uuid/uuid"
)

// Mover is a POSIX data mover
type Mover struct {
	name       string
	client     *client.Client
	archiveDir string
	archiveID  uint32
}

// PosixMover returns a new *Mover
func PosixMover(c *client.Client, archiveDir string, archiveID uint32) *Mover {
	return &Mover{
		name:       fmt.Sprintf("posix-%d", archiveID),
		client:     c,
		archiveDir: archiveDir,
		archiveID:  archiveID,
	}
}

// FsName returns the name of the associated Lustre filesystem
func (m *Mover) FsName() string {
	return m.client.FsName()
}

// ArchiveID returns the HSM archive number associated with this data mover
func (m *Mover) ArchiveID() uint32 {
	return m.archiveID
}

func newFileID() string {
	return uuid.New()
}

// CopyWithProgress initiates a movement of data with progress updates
func CopyWithProgress(dst io.WriterAt, src io.ReaderAt, start int64, length int64, action *dmplugin.Action) (int64, error) {
	debug.Printf("Copy %d %d", start, length)
	blockSize := 10 * 1024 * 1024 // FIXME: parameterize

	offset := start
	for offset < start+length {
		n, err := CopyAt(dst, src, offset, blockSize)
		offset += int64(n)
		if n < blockSize && err == io.EOF {
			break
		}

		if err != nil {
			return offset + int64(n), err
		}

		err = action.Update(offset-int64(n), int64(n), length)
		if err != nil {
			return offset, err
		}
	}
	return offset, nil
}

// Base returns the base path in which the mover is operating
func (m *Mover) Base() string {
	return m.client.Path()
}

func (m *Mover) destination(id string) string {
	dir := path.Join(m.archiveDir,
		"objects",
		fmt.Sprintf("%s", id[0:2]),
		fmt.Sprintf("%s", id[2:4]))

	err := os.MkdirAll(dir, 0600)
	if err != nil {
		alert.Fatal(err)
	}
	return path.Join(dir, id)
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// Archive fulfills an HSM Archive request
func (m *Mover) Archive(action *dmplugin.Action) error {
	debug.Printf("%s id:%d archive %s", m.name, action.ID(), action.PrimaryPath())
	start := time.Now()

	fileID := newFileID()

	src, err := os.Open(path.Join(m.Base(), action.PrimaryPath()))
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := os.Create(m.destination(fileID))
	if err != nil {
		return err
	}
	defer dst.Close()

	var length int64
	if uint64(action.Length()) == math.MaxUint64 {
		fi, err := src.Stat()
		if err != nil {
			return err
		}

		length = fi.Size() - action.Offset()
	} else {
		// TODO: Sanity check length + offset with actual file size?
		length = action.Length()
	}

	n, err := CopyWithProgress(dst, src, action.Offset(), length, action)
	if err != nil {
		debug.Printf("copy error %v read %d expected %d", err, n, length)
		return err
	}

	debug.Printf("%s id:%d Archived %d bytes in %v from %s to %s", m.name, action.ID(), n,
		time.Since(start),
		action.PrimaryPath(),
		m.destination(fileID))
	action.SetFileID([]byte(fileID))
	action.SetActualLength(uint64(n))
	return nil
}

// Restore fulfills an HSM Restore request
func (m *Mover) Restore(action *dmplugin.Action) error {
	debug.Printf("%s id:%d restore %s %s", m.name, action.ID(), action.PrimaryPath(), action.FileID())
	start := time.Now()

	if action.FileID() == "" {
		return errors.New("Missing file_id")
	}

	src, err := os.Open(m.destination(action.FileID()))
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := os.OpenFile(path.Join(m.Base(), action.WritePath()), os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer dst.Close()

	var length int64
	if uint64(action.Length()) == math.MaxUint64 {
		fi, err := src.Stat()
		if err != nil {
			return err
		}

		length = fi.Size() - action.Offset()
	} else {
		// TODO: Sanity check length + offset with actual file size?
		length = action.Length()
	}

	n, err := CopyWithProgress(dst, src, action.Offset(), length, action)
	if err != nil {
		debug.Printf("copy error %v read %d expected %d", err, n, length)
		return err
	}

	debug.Printf("%s id:%d Restored %d bytes in %v to %s", m.name, action.ID(), n,
		time.Since(start),
		action.PrimaryPath())
	action.SetActualLength(uint64(n))
	return nil
}

// Remove fulfills an HSM Remove request
func (m *Mover) Remove(action *dmplugin.Action) error {
	debug.Printf("%s: remove %s %s", m.name, action.PrimaryPath(), action.FileID())
	if action.FileID() == "" {
		return errors.New("Missing file_id")
	}

	return os.Remove(m.destination(action.FileID()))
}
