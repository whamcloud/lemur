package posix

import (
	"crypto/md5"
	"hash"
	"io"
)

type ChecksumWriter struct {
	dest io.WriterAt
	md5  hash.Hash
}

func NewChecksumWriter(dest io.WriterAt) *ChecksumWriter {
	return &ChecksumWriter{
		dest: dest,
		md5:  md5.New(),
	}
}

func (cw *ChecksumWriter) WriteAt(b []byte, off int64) (int, error) {
	cw.md5.Write(b)
	return cw.dest.WriteAt(b, off)
}

func (cw *ChecksumWriter) Sum() []byte {
	return cw.md5.Sum(nil)
}
