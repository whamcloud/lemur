// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package dmplugin

import (
	lustre "github.com/intel-hpdd/go-lustre"
	"github.com/intel-hpdd/logging/alert"
)

// Fataler provides Fatal and Fatalf
type Fataler interface {
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
}

// TestAction is an Action implementation used for testing Movers.
type TestAction struct {
	t            Fataler
	id           uint64
	path         string
	offset       int64
	length       int64
	data         []byte
	uuid         string
	hash         []byte
	url          string
	ActualLength int
	Updates      int
}

// NewTestAction returns a stub action that can be used for testing.
func NewTestAction(t Fataler, path string, offset int64, length int64, uuid string, data []byte) *TestAction {
	return &TestAction{
		t:      t,
		id:     1,
		path:   path,
		offset: offset,
		length: length,
		uuid:   uuid,
		data:   data,
	}
}

// Update sends an action status update
func (a *TestAction) Update(offset, length, max int64) error {
	a.Updates++
	return nil
}

// Complete signals that the action has completed
func (a *TestAction) Complete() error {
	return nil
}

// Fail signals that the action has failed
func (a *TestAction) Fail(err error) error {
	alert.Warnf("fail: id:%d %v", a.id, err)
	return nil
}

// ID returns the action item's ID
func (a *TestAction) ID() uint64 {
	return a.id
}

// Offset returns the current offset of the action item
func (a *TestAction) Offset() int64 {
	return a.offset
}

// Length returns the expected length of the action item's file
func (a *TestAction) Length() int64 {
	return a.length
}

// Data returns a byte slice of the action item's data
func (a *TestAction) Data() []byte {
	return a.data
}

// PrimaryPath returns the action item's primary file path
func (a *TestAction) PrimaryPath() string {
	return a.path
}

// WritePath returns the action item's write path (e.g. for restores)
func (a *TestAction) WritePath() string {
	return a.path
}

// UUID returns the action item's file id
func (a *TestAction) UUID() string {
	return a.uuid
}

// Hash returns the action item's file id
func (a *TestAction) Hash() []byte {
	return a.hash
}

// URL returns the action item's file id
func (a *TestAction) URL() string {
	return a.url
}

// SetUUID returns the action item's file id
func (a *TestAction) SetUUID(u string) {
	a.uuid = u
}

// SetHash sets the action's file id
func (a *TestAction) SetHash(id []byte) {
	a.hash = id
}

// SetURL returns the action item's file id
func (a *TestAction) SetURL(u string) {
	a.url = u
}

// SetActualLength sets the action's actual file length
func (a *TestAction) SetActualLength(length int64) {
	if a.length != lustre.MaxExtentLength && length != a.length {
		a.t.Fatalf("actual length does not match original %d != %d", length, a.length)
	}
	a.ActualLength = int(length)
}
