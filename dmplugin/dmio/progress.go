// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package dmio

import (
	"io"
	"sync/atomic"
	"time"

	"github.com/intel-hpdd/logging/alert"
)

// The default buffer size in io.copyBuffer() is 32KB -- this is the
// read size seen when the checksummer is running.
const ckSumSig = 32 * 1024

type (
	progressFunc func(int64, int64) error

	progressUpdater struct {
		done        chan struct{}
		bytesCopied int64
	}

	// ProgressReader wraps an io.ReaderAt and periodically invokes the
	// supplied callback to provide progress updates.
	ProgressReader struct {
		progressUpdater

		src io.ReadSeeker
	}

	// ProgressWriter wraps an io.Writer and periodically invokes the
	// supplied callback to provide progress updates.
	ProgressWriter struct {
		progressUpdater

		dst io.Writer
	}

	// ProgressWriterAt wraps an io.WriterAt and periodically invokes the
	// supplied callback to provide progress updates.
	ProgressWriterAt struct {
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

// Read calls internal Read and tracks how many bytes were read.
func (r *ProgressReader) Read(p []byte) (n int, err error) {
	n, err = r.src.Read(p)
	atomic.AddInt64(&r.bytesCopied, int64(n))
	return
}

// DISABLED
//
// The go http client package wraps the socket with a bufio.NewWriter() with
// the default 4k buffer. When aws sdk sends our file data, it ends up using
// io.Copy(w, src) to copy the data. This uses bufio Writer.ReadFrom() method
// and this reads from from our file into a 4k buf. One way to fix this could
// have been to implment a WriteTo method so we could read any size buffer we
// wanted to, but this doesn't work because the aws sdk has wrapped our file
// object with several others.
//
// One way to trick the sdk to read larger buffer sizes is to disable ReadAt and
// force the sdk to fall back to Read each chunk with one call. This is much
// better for lustre, but now read IO is single threaded, so this isn't so good
// either.  On the positive side, now the file is only read once as the sdk is
// able to sign each chunk from buffer in memeory, and also now we could
// calculate the sha1 for the whole file like we do in the posix mover.
//
// It is a shame that go-aws-sdk doesn't provide a callback for updating status
// like boto does.

// ReadAt reads len(p) bytes into p starting at offset off in the underlying
// input source. It returns the number of bytes read (0 <= n <= len(p)) and
// any error encountered.
/*
func (r *ProgressReader) ReadAt(p []byte, off int64) (int, error) {
	n, err := r.src.ReadAt(p, off)

	// Stupid hack to work around double-counting for progress updates.
	// Each file is read twice -- once for checksumming, then again
	// to actually transfer the data.
	if n != ckSumSig {
		atomic.AddInt64(&r.bytesCopied, n)
	}

	return n, err
}
*/

// NewProgressReader returns a new *ProgressReader
func NewProgressReader(src io.ReadSeeker, updateEvery time.Duration, f progressFunc) *ProgressReader {
	r := &ProgressReader{
		src: src,
	}

	r.startUpdates(updateEvery, f)

	return r
}

// Write writes len(p) bytes from p to the underlying data stream at
// offset off. It returns the number of bytes written from p (0 <= n <= len(p))
// and any error encountered that caused the write to stop early. WriteAt
// must return a non-nil error if it returns n < len(p).
func (w *ProgressWriter) Write(p []byte) (int, error) {
	n, err := w.dst.Write(p)

	atomic.AddInt64(&w.bytesCopied, int64(n))
	// debug.Printf("wrote %d bytes", n)
	return n, err

}

// NewProgressWriter returns a new *ProgressWriter
func NewProgressWriter(dst io.Writer, updateEvery time.Duration, f progressFunc) *ProgressWriter {
	w := &ProgressWriter{
		dst: dst,
	}
	w.startUpdates(updateEvery, f)

	return w
}

// WriteAt writes len(p) bytes from p to the underlying data stream at
// offset off. It returns the number of bytes written from p (0 <= n <= len(p))
// and any error encountered that caused the write to stop early. WriteAt
// must return a non-nil error if it returns n < len(p).
func (w *ProgressWriterAt) WriteAt(p []byte, off int64) (int, error) {
	n, err := w.dst.WriteAt(p, off)

	atomic.AddInt64(&w.bytesCopied, int64(n))

	return n, err
}

// NewProgressWriterAt returns a new *ProgressWriterAt
func NewProgressWriterAt(dst io.WriterAt, updateEvery time.Duration, f progressFunc) *ProgressWriterAt {
	w := &ProgressWriterAt{
		dst: dst,
	}
	w.startUpdates(updateEvery, f)

	return w
}
