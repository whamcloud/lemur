// Copyright (c) 2016 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package checksum

import (
	"crypto/sha1"
	"hash"
	"io"
	"os"

	"github.com/pkg/errors"
)

type (
	// Writer wraps an io.WriterAt and updates the checksum
	// with every write.
	Writer interface {
		io.Writer
		Sum() []byte
	}

	// Sha1HashWriter implements Writer and uses the SHA1
	// algorithm to calculate the file checksum
	Sha1HashWriter struct {
		dest  io.Writer
		cksum hash.Hash
	}

	// NoopHashWriter implements Writer but doesn't
	// actually calculate a checksum
	NoopHashWriter struct {
		dest io.Writer
	}
)

// NewSha1HashWriter returns a new Sha1HashWriter
func NewSha1HashWriter(dest io.Writer) Writer {
	return &Sha1HashWriter{
		dest:  dest,
		cksum: sha1.New(),
	}
}

// Write updates the checksum and writes the byte slice at offset
func (hw *Sha1HashWriter) Write(b []byte) (int, error) {
	_, err := hw.cksum.Write(b)
	if err != nil {
		return 0, errors.Wrap(err, "updating checksum failed")
	}
	return hw.dest.Write(b)
}

// Sum returns the checksum
func (hw *Sha1HashWriter) Sum() []byte {
	return hw.cksum.Sum(nil)
}

// NewNoopHashWriter returns a new NoopHashWriter
func NewNoopHashWriter(dest io.Writer) Writer {
	return &NoopHashWriter{
		dest: dest,
	}
}

// WriteAt writes the byte slice at offset
func (hw *NoopHashWriter) Write(b []byte) (int, error) {
	return hw.dest.Write(b)
}

// Sum returns a dummy checksum
func (hw *NoopHashWriter) Sum() []byte {
	return []byte{}
}

// FileSha1Sum returns the SHA1 checksum for the supplied file path
func FileSha1Sum(filePath string) ([]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to open %s for checksum", filePath)
	}
	defer file.Close()

	hash := sha1.New()
	_, err = io.Copy(hash, file)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to compute checksum for %s")
	}

	return hash.Sum(nil), nil
}
