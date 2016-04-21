package posix

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/pborman/uuid"
	"github.com/rcrowley/go-metrics"
	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/audit"
	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/policy/pdm/dmplugin"
	"github.intel.com/hpdd/policy/pkg/client"
)

var rate metrics.Meter

func init() {
	rate = metrics.NewMeter()

	// if debug.Enabled() {
	go func() {
		var lastCount int64
		for {
			if lastCount != rate.Count() {
				audit.Logf("total %s (1 min/5 min/15 min/inst): %s/%s/%s/%s msg/sec\n",
					humanize.Comma(rate.Count()),
					humanize.Comma(int64(rate.Rate1())),
					humanize.Comma(int64(rate.Rate5())),
					humanize.Comma(int64(rate.Rate15())),
					humanize.Comma(int64(rate.RateMean())),
				)
				lastCount = rate.Count()
			}
			time.Sleep(10 * time.Second)
		}
	}()
	// }
}

// Mover is a POSIX data mover
type Mover struct {
	name       string
	client     *client.Client
	archiveDir string
}

type FileID struct {
	Uuid string
	Sum  string
}

// PosixMover returns a new *Mover
func PosixMover(c *client.Client, archiveDir string, archiveID uint32) *Mover {
	if archiveDir == "" {
		panic("archiveDir is unset?!?")
	}

	return &Mover{
		name:       fmt.Sprintf("posix-%d", archiveID),
		client:     c,
		archiveDir: archiveDir,
	}
}

func newFileID() string {
	return uuid.New()
}

// CopyWithProgress initiates a movement of data with progress updates
func CopyWithProgress(dst io.WriterAt, src io.ReaderAt, start int64, length int64, action *dmplugin.Action) (int64, error) {
	var blockSize int64 = 10 * 1024 * 1024 // FIXME: parameterize

	offset := start
	for offset < start+length {
		n, err := CopyAt(dst, src, offset, blockSize)
		offset += n
		if n < blockSize && err == io.EOF {
			break
		}

		if err != nil {
			return offset, err
		}

		err = action.Update(offset-n, n, length)
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
	rate.Mark(1)
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

	cw := NewChecksumWriter(dst)

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

	n, err := CopyWithProgress(cw, src, action.Offset(), length, action)
	if err != nil {
		debug.Printf("copy error %v read %d expected %d", err, n, length)
		return err
	}

	debug.Printf("%s id:%d Archived %d bytes in %v from %s to %s %x", m.name, action.ID(), n,
		time.Since(start),
		action.PrimaryPath(),
		m.destination(fileID),
		cw.Sum())

	id := &FileID{
		Uuid: fileID,
		Sum:  fmt.Sprintf("%x", cw.Sum()),
	}
	buf, err := json.Marshal(id)
	if err != nil {
		return err
	}
	action.SetFileID(buf)
	action.SetActualLength(uint64(n))
	return nil
}

func parseFileID(buf []byte) (*FileID, error) {
	var id FileID
	err := json.Unmarshal(buf, &id)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

// Restore fulfills an HSM Restore request
func (m *Mover) Restore(action *dmplugin.Action) error {
	debug.Printf("%s id:%d restore %s %s", m.name, action.ID(), action.PrimaryPath(), action.FileID())
	rate.Mark(1)
	start := time.Now()

	if action.FileID() == "" {
		return errors.New("Missing file_id")
	}
	id, err := parseFileID([]byte(action.FileID()))
	if err != nil {
		return err
	}
	src, err := os.Open(m.destination(id.Uuid))
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := os.OpenFile(path.Join(m.Base(), action.WritePath()), os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer dst.Close()

	cw := NewChecksumWriter(dst)

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

	n, err := CopyWithProgress(cw, src, action.Offset(), length, action)
	if err != nil {
		debug.Printf("copy error %v read %d expected %d", err, n, length)
		return err
	}

	debug.Printf("%s id:%d Restored %d bytes in %v to %s %x", m.name, action.ID(), n,
		time.Since(start),
		action.PrimaryPath(),
		cw.Sum())
	action.SetActualLength(uint64(n))
	return nil
}

// Remove fulfills an HSM Remove request
func (m *Mover) Remove(action *dmplugin.Action) error {
	debug.Printf("%s: remove %s %s", m.name, action.PrimaryPath(), action.FileID())
	rate.Mark(1)
	if action.FileID() == "" {
		return errors.New("Missing file_id")
	}
	id, err := parseFileID([]byte(action.FileID()))
	if err != nil {
		return err
	}

	return os.Remove(m.destination(id.Uuid))
}
