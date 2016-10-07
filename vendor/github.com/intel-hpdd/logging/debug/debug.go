// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package debug

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strconv"
	"sync/atomic"

	"github.com/intel-hpdd/logging/external"
)

type (
	// Debugger wraps a *log.Logger with some configuration and
	// convenience methods
	Debugger struct {
		log     *log.Logger
		enabled int32
	}

	// Flag allows the flag package to enable debugging
	Flag bool
)

var std *Debugger

// EnableEnvVar is the name of an environment variable that, if set, will
// enable this package's functionality.
const EnableEnvVar = "ENABLE_DEBUG"

func init() {
	std = NewDebugger(os.Stderr)

	if os.Getenv(EnableEnvVar) != "" {
		Enable()
	}
}

// FlagVar returns a tuple of parameters suitable for flag.Var()
func FlagVar() (*Flag, string, string) {
	f := Flag(false)
	return &f, "debug", "enable debug output"
}

// IsBoolFlag satisfies the flag.boolFlag interface
func (f *Flag) IsBoolFlag() bool {
	return true
}

func (f *Flag) String() string {
	return fmt.Sprintf("%v", *f)
}

// Set satisfies the flag.Value interface
func (f *Flag) Set(value string) error {
	b, err := strconv.ParseBool(value)
	if err == nil {
		if b {
			std.Enable()
		}
		f = (*Flag)(&b)
	}
	return err
}

// NewDebugger creates a new *Debugger which logs to the supplied io.Writer
func NewDebugger(out io.Writer) *Debugger {
	return &Debugger{
		log: log.New(out, "DEBUG ", log.Lmicroseconds|log.Lshortfile),
	}
}

// Enabled indicates whether or not debugging is enabled
func (d *Debugger) Enabled() bool {
	return atomic.LoadInt32(&d.enabled) == 1
}

// Enable turns on debug logging
func (d *Debugger) Enable() {
	atomic.CompareAndSwapInt32(&d.enabled, 0, 1)
}

// Disable turns off debug logging
func (d *Debugger) Disable() {
	atomic.CompareAndSwapInt32(&d.enabled, 1, 0)
}

// Output writes the output for a logging event
func (d *Debugger) Output(skip int, s string) {
	if !d.Enabled() {
		return
	}
	d.log.Output(skip, s)
}

// Printf outputs formatted arguments
func (d *Debugger) Printf(f string, v ...interface{}) {
	if !d.Enabled() {
		return
	}
	d.Output(3, fmt.Sprintf(f, v...))
}

// Print outputs the arguments
func (d *Debugger) Print(v ...interface{}) {
	if !d.Enabled() {
		return
	}
	d.Output(3, fmt.Sprint(v...))
}

// Assertf accepts a boolean expression and formatted arguments, which
// if the expression is false, will be printed before panicing.
func (d *Debugger) Assertf(expr bool, f string, v ...interface{}) {
	if !d.Enabled() {
		return
	}
	if !expr {
		msg := fmt.Sprintf("ASSERTION FAILED: "+f, v...)
		d.Output(3, msg)
		panic(msg)
	}
}

// Assert accepts a boolean expression and arguments, which if the
// expression is false, will be printed before panicing.
func (d *Debugger) Assert(expr bool, v ...interface{}) {
	if !d.Enabled() {
		return
	}
	if !expr {
		msg := fmt.Sprintf("ASSERTION FAILED: %s", fmt.Sprint(v...))
		d.Output(3, msg)
		panic(msg)
	}
}

// SetOutput configures the output writer for the debugger's logger
func (d *Debugger) SetOutput(out io.Writer) {
	d.log.SetOutput(out)
}

// Writer returns a new *external.Writer suitable for injection into
// 3rd-party logging packages.
func (d *Debugger) Writer() *external.Writer {
	return external.NewWriter(d)
}

// Write implements io.Writer and allows the debugger to be used as
// a log writer.
func (d *Debugger) Write(data []byte) (int, error) {
	d.Output(5, string(data))

	return len(data), nil
}

// package-level functions follow

// Writer returns a new *external.Writer suitable for injection into
// 3rd-party logging packages.
func Writer() *external.Writer {
	return std.Writer()
}

// SetOutput configures the output writer for the wrapped *log.Logger
func SetOutput(out io.Writer) {
	std.SetOutput(out)
}

// Enable enables debug logging
func Enable() {
	std.Enable()
}

// Disable disables debug logging
func Disable() {
	std.Disable()
}

// Enabled returns a bool indicating whether or not debugging is enabled
func Enabled() bool {
	return std.Enabled()
}

// Output prints message if debug logging is enabled.
func Output(skip int, msg string) {
	std.Output(skip, msg)
}

// Printf prints message if debug logging is enabled.
func Printf(f string, v ...interface{}) {
	if !std.Enabled() {
		return
	}
	std.Output(3, fmt.Sprintf(f, v...))
}

// Print prints arguments if debug logging is enabled.
func Print(v ...interface{}) {
	if !std.Enabled() {
		return
	}
	std.Output(3, fmt.Sprint(v...))
}

// Assertf will panic if expression is not true, but only if debugging is enabled
func Assertf(expr bool, f string, v ...interface{}) {
	if !std.Enabled() {
		return
	}
	if !expr {
		msg := fmt.Sprintf("ASSERTION FAILED: "+f, v...)
		std.Output(3, msg)
		panic(msg)
	}
}

// Assert will panic if expression is not true, but only if debugging is enabled
func Assert(expr bool, v ...interface{}) {
	if !std.Enabled() {
		return
	}
	if !expr {
		msg := fmt.Sprintf("ASSERTION FAILED: %s", fmt.Sprint(v...))
		std.Output(3, msg)
		panic(msg)
	}
}

// Shell runs command only in debug mode.
func Shell(cmd string, args ...string) {
	if !std.Enabled() {
		return
	}
	c := exec.Command(cmd, args...)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	c.Run()
}
