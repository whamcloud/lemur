package audit

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.intel.com/hpdd/logging/external"
)

type (
	// Logger wraps a *log.Logger with some configuration and
	// convenience methods
	Logger struct {
		log *log.Logger
	}
)

var std *Logger

const logFlags = log.LstdFlags | log.LUTC

func init() {
	std = NewLogger(os.Stdout)
}

// NewLogger returns a *Logger
func NewLogger(out io.Writer) *Logger {
	return &Logger{
		log: log.New(out, "", logFlags),
	}
}

// SetOutput updates the embedded logger's output
func (l *Logger) SetOutput(out io.Writer) {
	l.log.SetOutput(out)
}

// Output writes the output for a logging event
func (l *Logger) Output(skip int, s string) {
	l.log.Output(skip, s)
}

// Log outputs a log message from the arguments
func (l *Logger) Log(v ...interface{}) {
	l.Output(3, fmt.Sprint(v...))
}

// Logf outputs a formatted log message from the arguments
func (l *Logger) Logf(f string, v ...interface{}) {
	l.Output(3, fmt.Sprintf(f, v...))
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

// Log outputs a log message from the arguments
func Log(v ...interface{}) {
	std.Output(3, fmt.Sprint(v...))
}

// Logf outputs a formatted log message from the arguments
func Logf(f string, v ...interface{}) {
	std.Output(3, fmt.Sprintf(f, v...))
}

// SetOutput updates the io.Writer for the package as well as any external
// writers created by the package
func SetOutput(out io.Writer) {
	std.SetOutput(out)
}
