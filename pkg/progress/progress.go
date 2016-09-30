package progress

import (
	"io"
	"sync/atomic"
	"time"

	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/debug"
)

// The default buffer size in io.copyBuffer() is 32KB -- this is the
// read size seen when the checksummer is running.
const ckSumSig = 32 * 1024

type (
	progressFunc func(uint64, uint64) error

	// ReaderAtSeeker groups the io.ReaderAt and io.Seeker interfaces
	ReaderAtSeeker interface {
		io.ReaderAt
		io.Reader
		io.Seeker
	}

	progressUpdater struct {
		done        chan struct{}
		bytesCopied uint64
	}

	// Reader wraps an io.ReaderAt and periodically invokes the
	// supplied callback to provide progress updates.
	Reader struct {
		progressUpdater

		src ReaderAtSeeker
	}

	// Writer wraps an io.WriterAt and periodically invokes the
	// supplied callback to provide progress updates.
	Writer struct {
		progressUpdater

		dst io.WriterAt
	}
)

// startUpdates creates a goroutine to periodically call the supplied
// callback with updated progress information. The callback must accept
// an int64 representing the last update value, and an int64 representing
// the delta between the last update value and the current bytes-copied count.
func (p *progressUpdater) startUpdates(updateEvery time.Duration, f progressFunc) {
	p.done = make(chan struct{})

	if updateEvery > 0 && f != nil {
		var lastTotal uint64
		go func() {
			for {
				select {
				case <-time.After(updateEvery):
					copied := atomic.LoadUint64(&p.bytesCopied)
					if err := f(lastTotal, copied-lastTotal); err != nil {
						alert.Warnf("Error received from updater callback: %s", err)
						// Should we return here?
					}
					lastTotal = copied
				case <-p.done:
					debug.Print("Shutting down updater goroutine")
					return
				}
			}
		}()
	}
}

// StopUpdates kills the updater goroutine
func (p *progressUpdater) StopUpdates() {
	p.done <- struct{}{}
}

// Seek calls the wrapped Seeker's Seek
func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	return r.src.Seek(offset, whence)
}

// Read calls the wrapped ReaderAt's ReadAt with offset 0
func (r *Reader) Read(p []byte) (int, error) {
	// Should we count these too?
	return r.src.Read(p)
}

// ReadAt reads len(p) bytes into p starting at offset off in the underlying
// input source. It returns the number of bytes read (0 <= n <= len(p)) and
// any error encountered.
func (r *Reader) ReadAt(p []byte, off int64) (int, error) {
	n, err := r.src.ReadAt(p, off)

	// Stupid hack to work around double-counting for progress updates.
	// Each file is read twice -- once for checksumming, then again
	// to actually transfer the data.
	if n != ckSumSig {
		atomic.AddUint64(&r.bytesCopied, uint64(n))
	}

	return n, err
}

// NewReader returns a new Reader
func NewReader(src ReaderAtSeeker, updateEvery time.Duration, f progressFunc) *Reader {
	r := &Reader{
		src: src,
	}

	r.startUpdates(updateEvery, f)

	return r
}

// WriteAt writes len(p) bytes from p to the underlying data stream at
// offset off. It returns the number of bytes written from p (0 <= n <= len(p))
// and any error encountered that caused the write to stop early. WriteAt
// must return a non-nil error if it returns n < len(p).
func (w *Writer) WriteAt(p []byte, off int64) (int, error) {
	n, err := w.dst.WriteAt(p, off)

	atomic.AddUint64(&w.bytesCopied, uint64(n))

	return n, err
}

// NewWriter returns a new Writer
func NewWriter(dst io.WriterAt, updateEvery time.Duration, f progressFunc) *Writer {
	w := &Writer{
		dst: dst,
	}
	w.startUpdates(updateEvery, f)

	return w
}
