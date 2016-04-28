package posix

import (
	"crypto/sha1"
	"hash"
	"io"
)

// ChecksumWriter wraps an io.WriterAt and updates the checksum
// with every write.
type ChecksumWriter struct {
	dest  io.WriterAt
	cksum hash.Hash
}

// NewChecksumWriter returns a new *ChecksumWriter
func NewChecksumWriter(dest io.WriterAt) *ChecksumWriter {
	return &ChecksumWriter{
		dest:  dest,
		cksum: sha1.New(),
	}
}

// WriteAt updates the checksum and writes the byte slice at offset
func (cw *ChecksumWriter) WriteAt(b []byte, off int64) (int, error) {
	cw.cksum.Write(b)
	return cw.dest.WriteAt(b, off)
}

// Sum returns the checksum
func (cw *ChecksumWriter) Sum() []byte {
	return cw.cksum.Sum(nil)
}
