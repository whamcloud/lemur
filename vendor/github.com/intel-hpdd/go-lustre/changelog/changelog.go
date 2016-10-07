// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package changelog

import (
	"fmt"
	"io"
	"time"

	"github.com/intel-hpdd/go-lustre"
	"github.com/intel-hpdd/go-lustre/llapi"
)

type (
	// Record represents a Lustre Changelog record
	Record interface {
		Index() int64
		Name() string
		Type() string
		TypeCode() uint
		Time() time.Time
		TargetFid() *lustre.Fid
		ParentFid() *lustre.Fid
		SourceName() string
		SourceFid() *lustre.Fid
		SourceParentFid() *lustre.Fid
		IsRename() bool
		IsLastRename() (bool, bool)
		IsLastUnlink() (bool, bool)
		JobID() string
		String() string
	}
	// Handle represents an interface to a Lustre Changelog
	Handle interface {
		Open(bool) error
		OpenAt(int64, bool) error
		Close() error
		NextRecord() (Record, error)
		Clear(string, int64) error
		String() string
	}

	// RecordIterator iterates over Records
	RecordIterator interface {
		NextRecord() (Record, error)
	}
)

// CreateHandle returns a Handle for accessing Changelog records
// on a given MDT.
func CreateHandle(device string) Handle {
	return &changelogHandle{
		device: device,
	}
}

// Clear is a convenience function to enable clearing a changelog
// without first creating a Handle.
func Clear(device, token string, endRec int64) error {
	return CreateHandle(device).Clear(token, endRec)
}

type changelogHandle struct {
	open   bool
	device string
	cl     *llapi.Changelog
}

// Open sets up the Changelog for reading from the first available record
func (h *changelogHandle) Open(follow bool) error {
	return h.OpenAt(1, follow)
}

// OpenAt sets up the Changelog for reading from the specified record index
func (h *changelogHandle) OpenAt(startRec int64, follow bool) error {
	var err error

	if h.open {
		return nil
	}

	h.cl, err = llapi.ChangelogStart(h.device, startRec, follow)
	if err != nil {
		h.cl = nil
		return err
	}

	h.open = true
	return nil
}

// Close closes the Changelog handle
func (h *changelogHandle) Close() error {
	h.open = false
	return llapi.ChangelogFini(h.cl)
}

// NextRecord retrieves the next available record
func (h *changelogHandle) NextRecord() (Record, error) {
	if !h.open {
		return nil, fmt.Errorf("NextRecord() called on closed handle")
	}
	return llapi.ChangelogRecv(h.cl)
}

// Clear clears Changelog records for the specified token up to the supplied
// end record index
func (h *changelogHandle) Clear(token string, endRec int64) error {
	return llapi.ChangelogClear(h.device, token, endRec)
}

func (h *changelogHandle) String() string {
	return h.device
}

// Follower is a Lustre Changelog follower. It provides a work-around
// for the broken CHANGELOG_FLAG_FOLLOW functionality in liblustreapi.
// If that is ever fixed, then it should be possible to just call
// NextRecord() in a blocking loop directly on the handle.
type Follower struct {
	handle    Handle
	records   chan Record
	err       chan error
	done      chan struct{}
	nextIndex int64
}

// Close signals that the Follower should close the wrapped Handle and
// stop processing records.
func (f *Follower) Close() {
	close(f.done)
}

// Follow opens the wrapped Handle at the first available index.
func (f *Follower) Follow() {
	f.FollowFrom(1)
}

// FollowFrom opens the wrapped Handle at the specified index.
func (f *Follower) FollowFrom(startRec int64) {
	f.nextIndex = startRec

	go func(h Handle) {
		for {
			if err := h.OpenAt(f.nextIndex, false); err != nil {
				f.err <- err
				return
			}

			r, err := h.NextRecord()
			for ; err == nil; r, err = h.NextRecord() {
				select {
				case <-f.done:
					h.Close()
					return
				case f.records <- r:
					f.nextIndex = r.Index() + 1
				}
			}
			if err != io.EOF {
				f.err <- err
				h.Close()
				return
			}
			h.Close()

			time.Sleep(1 * time.Second)
		}
	}(f.handle)
}

// NextRecord blocks until the next record is available or an error was
// encountered by the follower goroutine.
func (f *Follower) NextRecord() (Record, error) {
	select {
	case r := <-f.records:
		return r, nil
	case err := <-f.err:
		return nil, err
	case <-f.done:
		return nil, io.EOF
	}
}

// FollowHandle takes a Handle and wraps it with a Follower object.
func FollowHandle(h Handle, startRec int64) *Follower {
	f := &Follower{
		handle:  h,
		records: make(chan Record),
		done:    make(chan struct{}),
		err:     make(chan error),
	}

	f.FollowFrom(startRec)

	return f
}

// CreateFollower takes a MDT name and returns a Follower-wrapped Handle
func CreateFollower(device string, startRec int64) *Follower {
	h := CreateHandle(device)
	return FollowHandle(h, startRec)
}
