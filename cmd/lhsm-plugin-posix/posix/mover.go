// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package posix

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/intel-hpdd/lemur/dmplugin"
	"github.com/intel-hpdd/lemur/dmplugin/dmio"
	"github.com/intel-hpdd/lemur/pkg/checksum"
	"github.com/intel-hpdd/lemur/pkg/progress"
	"github.com/intel-hpdd/logging/alert"
	"github.com/intel-hpdd/logging/audit"
	"github.com/intel-hpdd/logging/debug"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/rcrowley/go-metrics"
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

// Should this be configurable?
const updateInterval = 10 * time.Second

type (

	// ArchiveConfig is configuration for one mover.
	ArchiveConfig struct {
		Name        string          `hcl:",key"`
		ID          int             `hcl:"id"`
		Root        string          `hcl:"root"`
		Compression string          `hcl:"compression"`
		Checksums   *ChecksumConfig `hcl:"checksums"`
	}

	// ArchiveSet is a list of mover configs.
	ArchiveSet []*ArchiveConfig

	// ChecksumConfig defines the configured behavior for file
	// checksumming in the POSIX data mover
	ChecksumConfig struct {
		Disabled                bool `hcl:"disabled"`
		DisableCompareOnRestore bool `hcl:"disable_compare_on_restore"`
	}
	// CompressionOption value determines  if data compression is enabled.
	CompressionOption int

	// Mover is a POSIX data mover
	Mover struct {
		Name        string
		ArchiveDir  string
		Compression CompressionOption
		Checksums   ChecksumConfig
	}

	// FileID is used to identify a file in the backend
	FileID struct {
		UUID string
		Sum  string `json:",omitempty"`
	}
)

const (
	// CompressOff disables data compression
	CompressOff CompressionOption = iota
	// CompressOn enables data compression
	CompressOn
	// CompressAuto enables compression when a compressible file is detection
	CompressAuto
)

var (
	// DefaultChecksums are enabled
	DefaultChecksums ChecksumConfig
)

func (a *ArchiveConfig) String() string {
	return fmt.Sprintf("%d:%s", a.ID, a.Root)
}

// CheckValid determines if the archive configuration is a valid one.
func (a *ArchiveConfig) CheckValid() error {
	var errs []string

	if a.Root == "" {
		errs = append(errs, fmt.Sprintf("Archive %s: archive root not set", a.Name))
	}

	if a.ID < 1 {
		errs = append(errs, fmt.Sprintf("Archive %s: archive id not set", a.Name))
	}

	if len(errs) > 0 {
		return errors.Errorf("Errors: %s", strings.Join(errs, ", "))
	}

	return nil
}

// CompressionOption parses Compression config parameter
func (a *ArchiveConfig) CompressionOption() CompressionOption {
	switch a.Compression {
	case "on":
		return CompressOn
	case "off":
		return CompressOff
	case "auto":
		return CompressOn // TODO: implement auto compresion
	default:
		return CompressOff
	}
}

// Merge the two configs and return a copy.
// Does not return nil, even if both a and other are nil.
func (a *ArchiveConfig) Merge(other *ArchiveConfig) *ArchiveConfig {
	var result ArchiveConfig
	if a != nil {
		result = *a
	}
	if other != nil {
		if other.Name != "" {
			result.Name = other.Name
		}
		if other.Root != "" {
			result.Root = other.Root
		}
		if other.Compression != "" {
			result.Compression = other.Compression
		}
		result.Checksums = result.Checksums.Merge(other.Checksums)
	} else {
		// Ensure we have a new copy of Checksums
		result.Checksums = result.Checksums.Merge(nil)
	}
	return &result
}

// Merge the two sets. Actually just returns the other one if set
// otherwise it returns the original set.
// TODO: actually merge the sets here
func (as ArchiveSet) Merge(other ArchiveSet) ArchiveSet {
	if len(other) > 0 {
		return other
	}
	return as
}

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
func NewMover(config *ArchiveConfig) (*Mover, error) {
	if config.Root == "" {
		return nil, errors.Errorf("Invalid mover config: ArchiveDir is unset")
	}

	return &Mover{
		Name:        config.Name,
		ArchiveDir:  config.Root,
		Compression: config.CompressionOption(),
		Checksums:   *DefaultChecksums.Merge(config.Checksums),
	}, nil
}

func newFileID() string {
	return uuid.New()
}

// CopyWithProgress initiates a movement of data with progress updates
func CopyWithProgress(dst io.Writer, src io.Reader, length int64, action dmplugin.Action) (uint64, error) {
	progressFunc := func(offset, n uint64) error {
		return action.Update(offset, n, uint64(length))
	}
	progressWriter := progress.NewWriter(dst, updateInterval, progressFunc)
	defer progressWriter.StopUpdates()

	n, err := io.Copy(progressWriter, src)

	return uint64(n), err
}

// ChecksumConfig returns the mover's checksum configuration
// Returns a pointer so the caller can modify the config.
func (m *Mover) ChecksumConfig() *ChecksumConfig {
	return &m.Checksums
}

// ChecksumEnabled returns true if user has enabled checksum calculation.
func (m *Mover) ChecksumEnabled() bool {
	return !m.Checksums.Disabled
}

// ChecksumWriter returns an instance of its namesake.
func (m *Mover) ChecksumWriter(dst io.Writer) (cw checksum.Writer) {
	if m.ChecksumEnabled() {
		cw = checksum.NewSha1HashWriter(dst)
	} else {
		cw = checksum.NewNoopHashWriter(dst)
	}
	return
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
	debug.Printf("%s id:%d ARCHIVE %s", m.Name, action.ID(), action.PrimaryPath())
	rate.Mark(1)
	start := time.Now()

	// Initialize Reader for Lustre file
	rdr, total, err := dmio.NewBufferedActionReader(action)
	if err != nil {
		return errors.Wrapf(err, "Could not create archive reader for %s", action)
	}
	defer rdr.Close()

	// If auto-compression enabled, determine "compressibility"
	enableZip := true
	if m.Compression != CompressOn {
		enableZip = false
	}

	// Initialize Writer for backing file
	fileID := newFileID()
	if enableZip {
		fileID += ".gz"
	}

	dst, err := os.Create(m.Destination(fileID))
	if err != nil {
		return errors.Wrapf(err, "%s: create backing file failed", m.Destination(fileID))
	}
	defer dst.Close()

	var cw checksum.Writer
	if enableZip {
		zip := gzip.NewWriter(dst)
		defer zip.Close()
		cw = m.ChecksumWriter(zip)
	} else {
		cw = m.ChecksumWriter(dst)
	}

	// Copy
	n, err := CopyWithProgress(cw, rdr, total, action)
	if err != nil {
		debug.Printf("copy error %v read %d expected %d", err, n, total)
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
	debug.Printf("%s id:%d RESTORE %s %s", m.Name, action.ID(), action.PrimaryPath(), action.FileID())
	rate.Mark(1)
	start := time.Now()

	// Initialize Reader for backing file
	if action.FileID() == nil {
		return errors.New("Missing file_id")
	}
	id, err := ParseFileID(action.FileID())
	if err != nil {
		return errors.Wrap(err, "parse file id")
	}

	enableUnzip := false
	if filepath.Ext(id.UUID) == ".gz" {
		debug.Printf("%s: id:%d decompressing %s", m.Name, action.ID(), id.UUID)
		enableUnzip = true
	}

	src, err := os.Open(m.Destination(id.UUID))
	if err != nil {
		return errors.Wrapf(err, "%s: open failed", m.Destination(id.UUID))
	}
	defer src.Close()

	var rdr io.Reader = bufio.NewReaderSize(src, dmio.BufferSize)

	if enableUnzip {
		unzip, er2 := gzip.NewReader(rdr)
		if er2 != nil {
			return errors.Wrap(er2, "gzip NewReader failed")
		}
		defer unzip.Close()
		rdr = unzip
	}

	// Initialize Writer for restore file on Lustre
	dst, err := os.OpenFile(action.WritePath(), os.O_WRONLY, 0644)
	if err != nil {
		return errors.Wrapf(err, "%s open write failed", action.WritePath())
	}
	defer dst.Close()

	length, err := dmio.ActualLength(action, dst)
	if err != nil {
		return errors.Wrap(err, "Unable to determine actual file length")
	}

	dst.Seek(int64(action.Offset()), 0)

	cw := m.ChecksumWriter(dst)

	// Copy
	n, err := CopyWithProgress(cw, rdr, length, action)
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
	debug.Printf("%s id:%d REMOVE %s %s", m.Name, action.ID(), action.PrimaryPath(), action.FileID())
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
