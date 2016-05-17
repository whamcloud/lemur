package posix

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/rcrowley/go-metrics"
	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/audit"
	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/policy/pdm/dmplugin"
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

type (
	// ChecksumConfig defines the configured behavior for file
	// checksumming in the POSIX data mover
	ChecksumConfig struct {
		Disabled                bool `hcl:"disabled"`
		DisableCompareOnRestore bool `hcl:"disable_compare_on_restore"`
	}

	// Mover is a POSIX data mover
	Mover struct {
		Name       string
		ArchiveDir string
		Checksums  ChecksumConfig
	}

	// FileID is used to identify a file in the backend
	FileID struct {
		UUID string
		Sum  string
	}
)

var (
	// DefaultChecksums are enabled
	DefaultChecksums ChecksumConfig
)

// Merge the two configurations. Returns a copy of
// other if it is not nil, otherwise retuns a copy of c.
func (c *ChecksumConfig) Merge(other *ChecksumConfig) *ChecksumConfig {
	var result ChecksumConfig
	if other != nil {
		result = *other

	} else if c != nil {
		result = *c
	} else {
		return nil
	}

	return &result
}

// NewMover returns a new *Mover
func NewMover(name string, dir string, checksums *ChecksumConfig) (*Mover, error) {
	if dir == "" {
		return nil, errors.Errorf("Invalid mover config: ArchiveDir is unset")
	}

	return &Mover{
		Name:       name,
		ArchiveDir: dir,
		Checksums:  *DefaultChecksums.Merge(checksums),
	}, nil
}

func newFileID() string {
	return uuid.New()
}

// CopyWithProgress initiates a movement of data with progress updates
func CopyWithProgress(dst io.WriterAt, src io.ReaderAt, start uint64, length uint64, action dmplugin.Action) (uint64, error) {
	var blockSize uint64 = 10 * 1024 * 1024 // FIXME: parameterize

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

		err = action.Update(offset-n, uint64(n), length)
		if err != nil {
			return offset, err
		}
	}
	return offset, nil
}

// ChecksumConfig returns the mover's checksum configuration
// Returns a pointer so the caller can modify the config.
func (m *Mover) ChecksumConfig() *ChecksumConfig {
	return &m.Checksums
}

// Destination returns the path to archived file.
// Exported for testing.
func (m *Mover) Destination(id string) string {
	dir := path.Join(m.ArchiveDir,
		"objects",
		fmt.Sprintf("%s", id[0:2]),
		fmt.Sprintf("%s", id[2:4]))

	err := os.MkdirAll(dir, 0700)
	if err != nil {
		alert.Abort(errors.Wrap(err, "mkdirall failed"))
	}
	return path.Join(dir, id)
}

// Start signals the mover to begin any asynchronous processing (e.g. stats)
func (m *Mover) Start() {
	debug.Printf("%s started", m.Name)
}

// Archive fulfills an HSM Archive request
func (m *Mover) Archive(action dmplugin.Action) error {
	debug.Printf("%s id:%d archive %s", m.Name, action.ID(), action.PrimaryPath())
	rate.Mark(1)
	start := time.Now()

	fileID := newFileID()

	src, err := os.Open(action.PrimaryPath())
	if err != nil {
		return errors.Wrapf(err, "%s: open failed", action.PrimaryPath())
	}
	defer src.Close()

	dst, err := os.Create(m.Destination(fileID))
	if err != nil {
		return errors.Wrapf(err, "%s: create failed", m.Destination(fileID))
	}
	defer dst.Close()

	var cw ChecksumWriter
	if !m.Checksums.Disabled {
		cw = NewSha1HashWriter(dst)
	} else {
		cw = NewNoopHashWriter(dst)
	}

	var length uint64
	if action.Length() == math.MaxUint64 {
		fi, err := src.Stat()
		if err != nil {
			return errors.Wrap(err, "stat failed")
		}

		length = uint64(fi.Size()) - action.Offset()
	} else {
		// TODO: Sanity check length + offset with actual file size?
		length = action.Length()
	}

	n, err := CopyWithProgress(cw, src, action.Offset(), length, action)
	if err != nil {
		debug.Printf("copy error %v read %d expected %d", err, n, length)
		return errors.Wrap(err, "copy failed")
	}

	debug.Printf("%s id:%d Archived %d bytes in %v from %s to %s %x", m.Name, action.ID(), n,
		time.Since(start),
		action.PrimaryPath(),
		m.Destination(fileID),
		cw.Sum())

	id := &FileID{
		UUID: fileID,
		Sum:  fmt.Sprintf("%x", cw.Sum()),
	}

	buf, err := EncodeFileID(id)
	if err != nil {
		return errors.Wrap(err, "encode file id failed")
	}
	action.SetFileID(buf)
	action.SetActualLength(n)
	return nil
}

// EncodeFileID is converts FileID to a json buffer.
func EncodeFileID(id *FileID) ([]byte, error) {
	buf, err := json.Marshal(id)
	if err != nil {
		return nil, errors.Wrap(err, "marshal failed")
	}
	return buf, nil

}

// ParseFileID unmarshalls the FileID struct from
// json encoded data received from the agent.
func ParseFileID(buf []byte) (*FileID, error) {
	var id FileID
	err := json.Unmarshal(buf, &id)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal failed")
	}
	return &id, nil
}

// Restore fulfills an HSM Restore request
func (m *Mover) Restore(action dmplugin.Action) error {
	debug.Printf("%s id:%d restore %s %s", m.Name, action.ID(), action.PrimaryPath(), action.FileID())
	rate.Mark(1)
	start := time.Now()

	if action.FileID() == nil {
		return errors.New("Missing file_id")
	}
	id, err := ParseFileID(action.FileID())
	if err != nil {
		return errors.Wrap(err, "parse file id")
	}
	src, err := os.Open(m.Destination(id.UUID))
	if err != nil {
		return errors.Wrapf(err, "%s: open failed", m.Destination(id.UUID))
	}
	defer src.Close()

	dst, err := os.OpenFile(action.WritePath(), os.O_WRONLY, 0644)
	if err != nil {
		return errors.Wrapf(err, "%s open write failed", action.WritePath())
	}
	defer dst.Close()

	var cw ChecksumWriter
	if id.Sum != "" && !m.Checksums.DisableCompareOnRestore {
		cw = NewSha1HashWriter(dst)
	} else {
		cw = NewNoopHashWriter(dst)
	}

	var length uint64
	if action.Length() == math.MaxUint64 {
		fi, err := src.Stat()
		if err != nil {
			return errors.Wrap(err, "stat failed")
		}

		length = uint64(fi.Size()) - action.Offset()
	} else {
		// TODO: Sanity check length + offset with actual file size?
		length = action.Length()
	}

	n, err := CopyWithProgress(cw, src, action.Offset(), length, action)
	if err != nil {
		debug.Printf("copy error %v read %d expected %d", err, n, length)
		return errors.Wrap(err, "copy failed")
	}

	if id.Sum != "" && !m.Checksums.DisableCompareOnRestore {
		if id.Sum != fmt.Sprintf("%x", cw.Sum()) {
			alert.Warnf("original checksum doesn't match new:  %s != %x", id.Sum, cw.Sum())
			return errors.New("Checksum mismatch!")
		}
	}

	debug.Printf("%s id:%d Restored %d bytes in %v to %s %x", m.Name, action.ID(), n,
		time.Since(start),
		action.PrimaryPath(),
		cw.Sum())
	action.SetActualLength(n)
	return nil
}

// Remove fulfills an HSM Remove request
func (m *Mover) Remove(action dmplugin.Action) error {
	debug.Printf("%s: remove %s %s", m.Name, action.PrimaryPath(), action.FileID())
	rate.Mark(1)
	if action.FileID() == nil {
		return errors.New("Missing file_id")
	}
	id, err := ParseFileID(action.FileID())
	if err != nil {
		return errors.Wrap(err, "parse file id failed")
	}

	return os.Remove(m.Destination(id.UUID))
}
