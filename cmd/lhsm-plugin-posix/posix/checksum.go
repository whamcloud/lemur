package posix

import (
	"crypto/sha1"
	"hash"
	"io"
)

type ChecksumWriter struct {
	dest  io.WriterAt
	cksum hash.Hash
}

func NewChecksumWriter(dest io.WriterAt) *ChecksumWriter {
	return &ChecksumWriter{
		dest:  dest,
		cksum: sha1.New(),
	}
}

func (cw *ChecksumWriter) WriteAt(b []byte, off int64) (int, error) {
	cw.cksum.Write(b)
	return cw.dest.WriteAt(b, off)
}

func (cw *ChecksumWriter) Sum() []byte {
	return cw.cksum.Sum(nil)
}
