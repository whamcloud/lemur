// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package logging

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/intel-hpdd/logging/alert"
	"github.com/intel-hpdd/logging/audit"
)

const (
	// LogFileFlags are suitable for appending to a log file
	LogFileFlags = os.O_CREATE | os.O_APPEND | os.O_RDWR

	// LogFileMode is suitable for root-only log access
	LogFileMode = 0600
)

// CreateWriter is a convenience function to ensure that the given input
// results in an io.Writer
func CreateWriter(w interface{}) (io.Writer, error) {
	switch w := w.(type) {
	case io.Writer:
		return w, nil
	case string:
		switch strings.ToLower(w) {
		case "stderr":
			return os.Stderr, nil
		case "stdout":
			return os.Stdout, nil
		case "":
			return ioutil.Discard, nil
		default:
			return os.OpenFile(w, LogFileFlags, LogFileMode)
		}
	default:
		return nil, fmt.Errorf("CreateWriter() called with unhandled input: %v", w)
	}
}

// SetWriter sets up the writer for non-interactive logging libraries
func SetWriter(w interface{}) error {
	writer, err := CreateWriter(w)
	if err != nil {
		return err
	}

	audit.SetOutput(writer)
	alert.SetOutput(writer)

	return nil
}
