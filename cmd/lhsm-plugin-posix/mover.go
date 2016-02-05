package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path"
	"time"

	"github.intel.com/hpdd/policy/pdm/dmplugin"
	"github.intel.com/hpdd/policy/pkg/client"
	"github.intel.com/hpdd/svclog"

	"code.google.com/p/go-uuid/uuid"
)

type Mover struct {
	name       string
	client     *client.Client
	archiveDir string
	archiveID  uint32
}

func PosixMover(c *client.Client, archiveDir string, archiveID uint32) *Mover {
	return &Mover{
		name:       fmt.Sprintf("posix-%d", archiveID),
		client:     c,
		archiveDir: archiveDir,
		archiveID:  archiveID,
	}
}

func (m *Mover) FsName() string {
	return m.client.FsName()
}

func (m *Mover) ArchiveID() uint32 {
	return m.archiveID
}

func newFileId() string {
	return uuid.New()
}

func CopyWithProgress(dst io.WriterAt, src io.ReaderAt, start int64, length int64, action *dmplugin.Action) (int64, error) {
	//	svclog.Debug("Copy %d %d", start, length)
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

func (m *Mover) Base() string {
	return m.client.Path()
}
func (h *Mover) destination(id string) string {
	dir := path.Join(h.archiveDir,
		"objects",
		fmt.Sprintf("%s", id[0:2]),
		fmt.Sprintf("%s", id[2:4]))

	err := os.MkdirAll(dir, 0600)
	if err != nil {
		log.Fatal(err)
	}
	return path.Join(dir, id)
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func (h *Mover) Archive(action *dmplugin.Action) error {
	svclog.Debug("%s id:%d archive %s", h.name, action.ID(), action.PrimaryPath())
	start := time.Now()

	fileId := newFileId()

	src, err := os.Open(path.Join(h.Base(), action.PrimaryPath()))
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := os.Create(h.destination(fileId))
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
		svclog.Debug("copy error %v read %d expected %d", err, n, length)
		return err
	}

	svclog.Debug("%s id:%d Archived %d bytes in %v from %s to %s", h.name, action.ID(), n,
		time.Since(start),
		action.PrimaryPath(),
		h.destination(fileId))
	action.SetFileID([]byte(fileId))
	action.SetActualLength(uint64(n))
	return nil
}

func (h *Mover) Restore(action *dmplugin.Action) error {
	svclog.Debug("%s id:%d restore %s %s", h.name, action.ID(), action.PrimaryPath(), action.FileID())
	start := time.Now()

	if action.FileID() == "" {
		return errors.New("Missing file_id")
	}

	src, err := os.Open(h.destination(action.FileID()))
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := os.OpenFile(path.Join(h.Base(), action.WritePath()), os.O_WRONLY, 0644)
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
		svclog.Debug("copy error %v read %d expected %d", err, n, length)
		return err
	}

	svclog.Debug("%s id:%d Restored %d bytes in %v to %s", h.name, action.ID(), n,
		time.Since(start),
		action.PrimaryPath())
	action.SetActualLength(uint64(n))
	return nil
}

func (h *Mover) Remove(action *dmplugin.Action) error {
	svclog.Debug("%s: remove %s %s", h.name, action.PrimaryPath(), action.FileID())
	if action.FileID() == "" {
		return errors.New("Missing file_id")
	}

	return os.Remove(h.destination(action.FileID()))
}
