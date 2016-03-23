package main

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
	progressFunc func(int64, int64) error

	// ReaderAtSeeker groups the io.ReaderAt and io.Seeker interfaces
	ReaderAtSeeker interface {
		io.ReaderAt
		io.Seeker
	}

	progressUpdater struct {
		done        chan struct{}
		bytesCopied int64
	}

	// ProgressReader wraps an io.ReaderAt and periodically invokes the
	// supplied callback to provide progress updates.
	ProgressReader struct {
		progressUpdater

		src ReaderAtSeeker
	}

	// ProgressWriter wraps an io.WriterAt and periodically invokes the
	// supplied callback to provide progress updates.
	ProgressWriter struct {
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
		var lastTotal int64
		go func() {
			for {
				select {
				case <-time.After(updateEvery):
					copied := atomic.LoadInt64(&p.bytesCopied)
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
func (r *ProgressReader) Seek(offset int64, whence int) (int64, error) {
	return r.src.Seek(offset, whence)
}

// Read calls the wrapped ReaderAt's ReadAt with offset 0
func (r *ProgressReader) Read(p []byte) (int, error) {
	return r.src.ReadAt(p, 0)
}

// ReadAt reads len(p) bytes into p starting at offset off in the underlying
// input source. It returns the number of bytes read (0 <= n <= len(p)) and
// any error encountered.
func (r *ProgressReader) ReadAt(p []byte, off int64) (int, error) {
	n, err := r.src.ReadAt(p, off)

	// Stupid hack to work around double-counting for progress updates.
	// Each file is read twice -- once for checksumming, then again
	// to actually transfer the data.
	if n != ckSumSig {
		atomic.AddInt64(&r.bytesCopied, int64(n))
	}

	return n, err
}

// NewProgressReader returns a new ProgressReader
func NewProgressReader(src ReaderAtSeeker, updateEvery time.Duration, f progressFunc) *ProgressReader {
	r := &ProgressReader{
		src: src,
	}
	r.startUpdates(updateEvery, f)

	return r
}

// WriteAt writes len(p) bytes from p to the underlying data stream at
// offset off. It returns the number of bytes written from p (0 <= n <= len(p))
// and any error encountered that caused the write to stop early. WriteAt
// must return a non-nil error if it returns n < len(p).
func (w *ProgressWriter) WriteAt(p []byte, off int64) (int, error) {
	n, err := w.dst.WriteAt(p, off)

	atomic.AddInt64(&w.bytesCopied, int64(n))

	return n, err
}

// NewProgressWriter returns a new ProgressWriter
func NewProgressWriter(dst io.WriterAt, updateEvery time.Duration, f progressFunc) *ProgressWriter {
	w := &ProgressWriter{
		dst: dst,
	}
	w.startUpdates(updateEvery, f)

	return w
}
