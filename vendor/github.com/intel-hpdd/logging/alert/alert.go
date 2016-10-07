// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package alert

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/intel-hpdd/logging/external"
)

type (
	// Logger wraps a *log.Logger with some configuration and
	// convenience methods
	Logger struct {
		log *log.Logger
	}
)

var std *Logger

// Provide as much information as possible about where the message originated,
// as this package should usually only be involved where there is a failure.
const logFlags = log.Ldate | log.Ltime | log.LUTC | log.Llongfile

func init() {
	std = NewLogger(os.Stderr)
}

// NewLogger returns a *Logger
func NewLogger(out io.Writer) *Logger {
	return &Logger{
		log: log.New(out, "ALERT ", logFlags),
	}
}

// SetFlags sets the output flags for the embedded logger
func (l *Logger) SetFlags(flags int) {
	l.log.SetFlags(flags)
}

// SetOutput updates the embedded logger's output
func (l *Logger) SetOutput(out io.Writer) {
	l.log.SetOutput(out)
}

// Output writes the output for a logging event
func (l *Logger) Output(skip int, s string) {
	l.log.Output(skip, s)
}

// Warn outputs a log message from the arguments
func (l *Logger) Warn(v ...interface{}) {
	l.Output(3, fmt.Sprint(v...))
}

// Warnf outputs a formatted log message from the arguments
func (l *Logger) Warnf(f string, v ...interface{}) {
	l.Output(3, fmt.Sprintf(f, v...))
}

// Fatal outputs a log message from the arguments, then exits
func (l *Logger) Fatal(v ...interface{}) {
	l.Output(3, fmt.Sprint(v...))
	os.Exit(1)
}

// Fatalf outputs a formatted log message from the arguments, then exits
func (l *Logger) Fatalf(f string, v ...interface{}) {
	l.Output(3, fmt.Sprintf(f, v...))
	os.Exit(1)
}

// Writer returns a new *external.Writer suitable for injection into
// 3rd-party logging packages.
func (l *Logger) Writer() *external.Writer {
	return external.NewWriter(l)
}

// Write implements io.Writer and allows the logger to be used as
// an embedded log writer.
func (l *Logger) Write(data []byte) (int, error) {
	l.Output(5, string(data))

	return len(data), nil
}

// package-level functions follow

// Writer returns a new *external.Writer suitable for injection into
// 3rd-party logging packages.
func Writer() *external.Writer {
	return std.Writer()
}

// SetOutput configures the output writer for the logger
func SetOutput(out io.Writer) {
	std.SetOutput(out)
}

// Warn outputs a log message from the arguments
func Warn(v ...interface{}) {
	std.Output(3, fmt.Sprint(v...))
}

// Warnf outputs a formatted log message from the arguments
func Warnf(f string, v ...interface{}) {
	std.Output(3, fmt.Sprintf(f, v...))
}

// Fatal outputs a log message from the arguments, then exits
func Fatal(v ...interface{}) {
	std.Output(3, fmt.Sprint(v...))
	os.Exit(1)
}

// Fatalf outputs a formatted log message from the arguments, then exits
func Fatalf(f string, v ...interface{}) {
	std.Output(3, fmt.Sprintf(f, v...))
	os.Exit(1)
}

// Abort prints error trace and exits
func Abort(err error) {
	// We don't need to see where the abort was called, so we remove
	// this flag before logging and exiting.
	std.SetFlags(logFlags &^ log.Llongfile)
	msg := fmt.Sprintf("%+v", err)

	std.Output(3, "Aborting program execution due to error(s):\n"+msg)
	os.Exit(1)
}
