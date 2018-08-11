// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package external

import (
	"fmt"
	"io"
	"log"
)

type (
	// Writer is an optionally-prefixed writer for
	// 3rd-party logging packages.
	Writer struct {
		log *log.Logger
	}
)

// NewWriter wraps an io.Writer with a *log.Logger which doesn't
// add any prefixing by default.
func NewWriter(out io.Writer) *Writer {
	return &Writer{
		log: log.New(out, "", 0),
	}
}

// SetOutput updates the embedded *log.Logger's output
func (w *Writer) SetOutput(out io.Writer) {
	w.log.SetOutput(out)
}

// Prefix optionally sets a logging prefix for the writer
func (w *Writer) Prefix(prefix string) *Writer {
	w.log.SetPrefix(prefix)
	return w
}

func (w *Writer) Write(data []byte) (int, error) {
	w.log.Output(3, string(data))

	return len(data), nil
}

// Log implements the aws.Logger interface, among others
func (w *Writer) Log(v ...interface{}) {
	w.log.Output(3, fmt.Sprint(v...))
}
