package dmio

import (
	"bufio"
	"io"
	"os"

	lustre "github.com/intel-hpdd/go-lustre"
	"github.com/intel-hpdd/lemur/dmplugin"
	"github.com/pkg/errors"
)

// BufferSize is the buffered reader size
var BufferSize = 1024 * 1024

type (
	statter interface {
		Stat() (os.FileInfo, error)
	}

	writerWriterAt interface {
		io.WriterAt
		io.Writer
	}

	// ActionReader wraps an io.SectionReader and also implements
	// io.Closer by closing the embedded io.Closer.
	ActionReader struct {
		sr     *io.SectionReader
		closer io.Closer
	}

	// BufferedActionReader wraps a buffered ActionReader and
	// also implements io.Closer by closing the embedded io.Closer.
	BufferedActionReader struct {
		br     *bufio.Reader
		closer io.Closer
	}

	// ActionWriter wraps an io.WriterAt but imposes a base offset
	// determined by the action's extent.
	ActionWriter struct {
		baseOffset int64
		wwa        writerWriterAt
		statter    statter
		closer     io.Closer
	}
)

// Close calls the embedded io.Closer's Close()
func (ar *ActionReader) Close() error {
	return ar.closer.Close()
}

// Read calls the embedded *io.SectionReader's Read()
func (ar *ActionReader) Read(p []byte) (int, error) {
	return ar.sr.Read(p)
}

// Seek calls the embedded *io.SectionReader's Seek()
func (ar *ActionReader) Seek(offset int64, whence int) (int64, error) {
	return ar.sr.Seek(offset, whence)
}

// Close calls the embedded io.Closer's Close()
func (bar *BufferedActionReader) Close() error {
	return bar.closer.Close()
}

// Read calls the embedded *bufio.Reader's Read()
func (bar *BufferedActionReader) Read(p []byte) (int, error) {
	return bar.br.Read(p)
}

// Close calls the embedded io.Closer's Close()
func (aw *ActionWriter) Close() error {
	return aw.closer.Close()
}

// WriteAt calls the embedded io.WriterAt's WriteAt(), with the
// offset adjusted by the base offset.
func (aw *ActionWriter) WriteAt(p []byte, off int64) (int, error) {
	return aw.wwa.WriteAt(p, aw.baseOffset+off)
}

// Write calls the embedded io.Writer's Write().
func (aw *ActionWriter) Write(p []byte) (int, error) {
	return aw.wwa.Write(p)
}

// Stat calls the embedded statter's Stat().
func (aw *ActionWriter) Stat() (os.FileInfo, error) {
	return aw.statter.Stat()
}

// ActualLength returns the length embedded in the action if it is not
// Inf (i.e. when it's an extent). Otherwise, interpret it as EOF
// and stat the actual file to determine the length on disk.
func ActualLength(action dmplugin.Action, fp statter) (int64, error) {
	var length int64
	if action.Length() == lustre.MaxExtentLength {
		fi, err := fp.Stat()
		if err != nil {
			return 0, errors.Wrap(err, "stat failed")
		}

		length = fi.Size() - int64(action.Offset())
	} else {
		// TODO: Sanity check length + offset with actual file size?
		length = int64(action.Length())
	}
	return length, nil
}

// NewBufferedActionReader returns a *BufferedActionReader for the supplied
// action.
func NewBufferedActionReader(action dmplugin.Action) (*BufferedActionReader, int64, error) {
	ar, length, err := NewActionReader(action)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Failed to create ActionReader from %s", action)
	}

	return &BufferedActionReader{
		br:     bufio.NewReaderSize(ar, BufferSize),
		closer: ar,
	}, length, nil
}

// NewActionReader returns an *ActionReader for the supplied action.
func NewActionReader(action dmplugin.Action) (*ActionReader, int64, error) {
	src, err := os.Open(action.PrimaryPath())
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Failed to open %s for read", action.PrimaryPath())
	}

	length, err := ActualLength(action, src)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Could not determine extent length for %s", action)
	}

	return &ActionReader{
		sr:     io.NewSectionReader(src, int64(action.Offset()), length),
		closer: src,
	}, length, nil
}

// NewActionWriter returns an *ActionWriter for the supplied action.
func NewActionWriter(action dmplugin.Action) (*ActionWriter, error) {
	dst, err := os.OpenFile(action.WritePath(), os.O_WRONLY, 0644)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to open %s for write", action.WritePath())
	}

	// Set up for simple Write()
	dst.Seek(int64(action.Offset()), 0)

	return &ActionWriter{
		baseOffset: int64(action.Offset()),
		wwa:        dst,
		statter:    dst,
		closer:     dst,
	}, nil
}
