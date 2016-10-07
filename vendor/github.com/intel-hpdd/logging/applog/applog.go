// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package applog

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	"github.com/intel-hpdd/logging"
	"github.com/intel-hpdd/logging/debug"

	"github.com/briandowns/spinner"
)

var (
	taskSuffix = " ... "
	std        *AppLogger
)

func init() {
	std = New()
}

type displayLevel int

func (d displayLevel) String() string {
	switch d {
	case DEBUG:
		return "DEBUG"
	case TRACE:
		return "TRACE"
	case USER:
		return "USER"
	case WARN:
		return "WARN"
	case FAIL:
		return "FAIL"
	case SILENT:
		return "SILENT"
	default:
		return fmt.Sprintf("Unknown level: %d", d)
	}
}

const (
	// DEBUG shows all
	DEBUG displayLevel = iota
	// TRACE shows application flow, suitable for support
	TRACE
	// USER shows user-appropriate messages
	USER
	// WARN shows warnings
	WARN
	// FAIL is bad
	FAIL
	// SILENT shows nothing
	SILENT
)

// LoggedWriter implements io.Writer and is used to redirect logging from
// 3rd-party libraries to this library.
type LoggedWriter struct {
	level  displayLevel
	prefix string
	logger *AppLogger
}

// Write logs the data at the specified loglevel
func (w *LoggedWriter) Write(data []byte) (int, error) {
	msg := string(data)
	if len(w.prefix) > 0 {
		msg = fmt.Sprintf("%s %s", w.prefix, data)
	}
	w.logger.logAt(w.level, msg)

	return len(data), nil
}

// Prefix optionally sets the LoggedWriter prefix
func (w *LoggedWriter) Prefix(prefix string) *LoggedWriter {
	w.prefix = prefix
	return w
}

// Level optionally sets the LoggedWriter log level
func (w *LoggedWriter) Level(level displayLevel) *LoggedWriter {
	w.level = level
	return w
}

// OptSetter sets logger options
type OptSetter func(*AppLogger)

// JournalFile configures the logger's journaler
func JournalFile(w interface{}) OptSetter {
	writer, err := logging.CreateWriter(w)
	if err != nil {
		panic(fmt.Errorf("Failed to create writer from %v: %s", w, err))
	}

	return func(l *AppLogger) {
		l.journal = log.New(writer, "", log.LstdFlags)
	}
}

// DisplayLevel sets the logger's display level
func DisplayLevel(d displayLevel) OptSetter {
	return func(l *AppLogger) {
		l.Level = d
	}
}

// New returns a new AppLogger
func New(options ...OptSetter) *AppLogger {
	logger := &AppLogger{
		spinner: spinner.New(spinner.CharSets[9], 100*time.Millisecond),
		out:     os.Stdout,
		err:     os.Stderr,
		Level:   USER,
		journal: log.New(ioutil.Discard, "", log.LstdFlags),
	}

	for _, option := range options {
		option(logger)
	}

	return logger
}

// AppLogger is a logger with methods for displaying entries to the user
// after recording them to a journal.
type AppLogger struct {
	Level displayLevel

	mu          sync.Mutex
	spinner     *spinner.Spinner
	out         io.Writer
	err         io.Writer
	lastEntry   string
	currentTask string
	journal     *log.Logger
}

func (l *AppLogger) logAt(level displayLevel, msg string) {
	switch level {
	case SILENT:
		return
	case TRACE:
		l.Trace(msg)
	case USER:
		l.User(msg)
	case WARN:
		l.Warn(msg)
	case FAIL:
		l.Fail(msg)
	default:
		l.Debug(msg)
	}
}

// Writer returns an io.Writer for injecting our logging into third-party
// libraries
func (l *AppLogger) Writer() *LoggedWriter {
	return &LoggedWriter{
		level:  DEBUG,
		prefix: "",
		logger: l,
	}
}

// DisplayLevel sets the logger's display level
func (l *AppLogger) DisplayLevel(level displayLevel) {
	DisplayLevel(level)(l)
}

// JournalFile configures the logger's journaler
func (l *AppLogger) JournalFile(w interface{}) {
	JournalFile(w)(l)
}

func (l *AppLogger) setLastEntry(entry string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lastEntry = entry
}

func (l *AppLogger) getLastEntry() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastEntry
}

func (l *AppLogger) recordEntry(level displayLevel, v ...interface{}) {
	if len(v) == 0 {
		return
	}

	switch arg := v[0].(type) {
	case error:
		l.setLastEntry(fmt.Sprintf("ERROR: %s", arg))
	case string:
		if len(v) > 1 {
			l.setLastEntry(fmt.Sprintf(arg, v[1:]...))
		} else {
			l.setLastEntry(fmt.Sprint(arg))
		}
	case fmt.Stringer:
		l.setLastEntry(fmt.Sprint(arg))
	default:
		l.setLastEntry(fmt.Sprintf("unknown type in recordEntry: %s", v))
	}
	l.journal.Printf("%s: %s", level, l.getLastEntry())
}

// Debug logs the entry and prints to stdout if level <= DEBUG
func (l *AppLogger) Debug(v ...interface{}) {
	l.recordEntry(DEBUG, v...)

	if l.Level <= DEBUG {
		fmt.Fprintf(l.out, "%s: %s\n", DEBUG, l.getLastEntry())
	}
}

// Trace logs the entry and prints to stdout if level <= TRACE
func (l *AppLogger) Trace(v ...interface{}) {
	l.recordEntry(TRACE, v...)

	if l.Level <= TRACE {
		fmt.Fprintf(l.out, "%s: %s\n", TRACE, l.getLastEntry())
	}
}

// User logs the entry and prints to stdout if level <= USER
func (l *AppLogger) User(v ...interface{}) {
	l.recordEntry(USER, v...)

	if l.Level <= USER {
		fmt.Fprintln(l.out, l.getLastEntry())
	}
}

// StartTask logs the entry at USER level and displays a spinner
// for long-running tasks
func (l *AppLogger) StartTask(v ...interface{}) {
	// Allow new tasks to display completion for previous tasks.
	if l.currentTask != "" {
		l.CompleteTask()
	}

	l.recordEntry(USER, v...)

	if l.Level == USER {
		l.currentTask = l.getLastEntry()
		// Don't fill log files with tons of spinner spam!
		if !WriterIsTerminal(l.out) {
			fmt.Fprint(l.out, l.currentTask+taskSuffix)
			return
		}
		l.spinner.Prefix = l.currentTask + taskSuffix
		l.spinner.Restart()
	}
}

// CompleteTask stops the spinner and prints a newline
func (l *AppLogger) CompleteTask(v ...interface{}) {
	l.spinner.Stop()

	if len(v) == 0 {
		l.recordEntry(USER, l.currentTask+taskSuffix+"Done.")
	} else {
		if fmtStr, ok := v[0].(string); ok {
			var newArgs []interface{}
			newArgs = append(newArgs, l.currentTask+taskSuffix+fmtStr)
			newArgs = append(newArgs, v[1:]...)
			l.recordEntry(USER, newArgs...)
		} else {
			l.recordEntry(USER, v...)
		}
	}

	if l.currentTask != "" && l.Level == USER {
		fmt.Fprintln(l.out, l.getLastEntry())
		l.currentTask = ""
	}
}

// Warn logs the entry and prints to stderr if level <= WARN
func (l *AppLogger) Warn(v ...interface{}) {
	l.recordEntry(WARN, v...)

	l.spinner.Stop()
	l.currentTask = ""
	if l.Level <= WARN {
		fmt.Fprintf(l.err, "%s: %s", WARN, l.getLastEntry())
	}
}

// Fail logs the entry and prints to stderr if level <= FAIL
func (l *AppLogger) Fail(v ...interface{}) {
	l.recordEntry(FAIL, v...)

	l.spinner.Stop()
	l.currentTask = ""
	if l.Level <= FAIL {
		fmt.Fprintln(l.err, l.getLastEntry())
	}
	os.Exit(1)
}

// StandardLogger returns the standard logger configured by the library
func StandardLogger() *AppLogger {
	return std
}

// SetStandard sets the standard logger to the supplied logger
func SetStandard(l *AppLogger) {
	std = l
}

// SetJournal sets the standard logger's journal writer
func SetJournal(w interface{}) {
	JournalFile(w)(std)
}

// SetLevel sets the standard logger's display level
func SetLevel(d displayLevel) {
	DisplayLevel(d)(std)

	// Enable debug logging for anything using our debug library
	if d == DEBUG {
		debug.Enable()
		debug.SetOutput(Writer())
	}
}

// Debug logs the entry and prints to stdout if level <= DEBUG
func Debug(v ...interface{}) {
	std.Debug(v...)
}

// Trace logs the entry and prints to stdout if level <= TRACE
func Trace(v ...interface{}) {
	std.Trace(v...)
}

// User logs the entry and prints to stdout if level <= USER
func User(v ...interface{}) {
	std.User(v...)
}

// Warn logs the entry and prints to stderr if level <= WARN
func Warn(v ...interface{}) {
	std.Warn(v...)
}

// Fail logs the entry and prints to stderr if level <= FAIL
func Fail(v ...interface{}) {
	std.Fail(v...)
}

// StartTask logs the entry at USER level and displays a spinner
// for long-running tasks
func StartTask(v ...interface{}) {
	std.StartTask(v...)
}

// CompleteTask stops the spinner and prints a newline
func CompleteTask(v ...interface{}) {
	std.CompleteTask(v...)
}

// Writer returns an io.Writer for injecting our logging into 3rd-party
// libraries
func Writer() *LoggedWriter {
	return std.Writer()
}
