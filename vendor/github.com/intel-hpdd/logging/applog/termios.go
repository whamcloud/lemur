// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package applog

import (
	"io"
	"os"
)

// WriterIsTerminal returns true if the given io.Writer converts to
// an *os.File and the file's fd is a terminal.
func WriterIsTerminal(writer io.Writer) bool {
	file, ok := writer.(*os.File)
	return ok && isTerminal(file.Fd())
}

// IsTerminal returns true if the given file descriptor is a terminal.
// Swiped from golang.org/x/crypto/ssh/terminal
func IsTerminal(fd int) bool {
	return isTerminal(uintptr(fd))
}
