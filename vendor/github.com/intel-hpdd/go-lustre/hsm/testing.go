// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package hsm

import (
	"fmt"
	"math/rand"
	"time"

	"golang.org/x/net/context"

	"github.com/intel-hpdd/logging/debug"
	"github.com/intel-hpdd/go-lustre"
	"github.com/intel-hpdd/go-lustre/llapi"
)

type (
	// SignalChan is a channel that waiters can block on while
	// waiting for certain events to occur.
	SignalChan chan struct{}

	// TestSource implements hsm.ActionSource, but provides a
	// Lustre-independent way of generating hsm requests.
	TestSource struct {
		outgoing chan ActionRequest
		rng      *rand.Rand
	}

	// TestRequest implements hsm.ActionRequest with additional
	// methods for controlling/inpecting request state.
	TestRequest struct {
		cookie                 uint64
		archive                uint
		action                 llapi.HsmAction
		extent                 llapi.HsmExtent
		testFid                *lustre.Fid
		handleProgressReceived chan *TestProgressUpdate
		data                   []byte
	}

	// TestProgressUpdate contains information about progress updates
	// received by the test handle.
	TestProgressUpdate struct {
		Cookie   uint64
		Offset   uint64
		Length   uint64
		Total    uint64
		Flags    int
		Errval   int
		Complete bool
	}
)

var (
	nextCookie uint64 = 0x1000
)

func (p *TestProgressUpdate) String() string {
	return fmt.Sprintf("Progress: cookie: 0x%x (%d:%d) total: %d complete: %v", p.Cookie, p.Offset, p.Length, p.Total, p.Complete)
}

// NewTestSource returns an ActionSource implementation suitable for testing
func NewTestSource() *TestSource {
	return &TestSource{
		outgoing: make(chan ActionRequest),
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Inject allows test code to inject arbitrary ActionRequests.
func (s *TestSource) Inject(ar ActionRequest) {
	s.outgoing <- ar
}

// GenerateRandomAction generates a random action request
func (s *TestSource) GenerateRandomAction() {
	s.Inject(&TestRequest{})
}

// Actions returns a channel for callers to receive ActionRequests
func (s *TestSource) Actions() <-chan ActionRequest {
	return s.outgoing
}

func (s *TestSource) closer(ctx context.Context) {
	<-ctx.Done()
	debug.Print("Shutting down test action generator")
	close(s.outgoing)
}

// Start starts the action generator
func (s *TestSource) Start(ctx context.Context) error {
	go s.closer(ctx)

	return nil
}

// NewTestRequest returns a new *TestRequest
func NewTestRequest(archive uint, action llapi.HsmAction, fid *lustre.Fid, data []byte) *TestRequest {
	nextCookie++
	return &TestRequest{
		cookie:                 nextCookie,
		testFid:                fid,
		archive:                archive,
		action:                 action,
		extent:                 llapi.HsmExtent{0, 0},
		handleProgressReceived: make(chan *TestProgressUpdate),
		data: data,
	}
}

func (r *TestRequest) String() string {
	return fmt.Sprintf("TEST %s %s %s 0x%x %s", r.action, r.testFid, r.extent, r.cookie, r.data)
}

// Begin returns a new test handle
func (r *TestRequest) Begin(flags int, isError bool) (ActionHandle, error) {
	return r, nil
}

// FailImmediately immediately fails the request
func (r *TestRequest) FailImmediately(errval int) {
	return
}

// ArchiveID returns the backend archive number
func (r *TestRequest) ArchiveID() uint {
	return r.archive
}

// Action returns the HSM action type
func (r *TestRequest) Action() llapi.HsmAction {
	return r.action
}

// Test-only methods for TestRequest follow

// ProgressUpdates returns a channel through which progress updates
// may be received.
func (r *TestRequest) ProgressUpdates() chan *TestProgressUpdate {
	return r.handleProgressReceived
}

// Progress updates current state of data transfer request.
func (r *TestRequest) Progress(offset, length, total uint64, flags int) error {
	r.handleProgressReceived <- &TestProgressUpdate{
		Cookie: r.cookie,
		Offset: offset,
		Length: length,
		Flags:  flags,
		Total:  total,
	}
	return nil
}

// End completes an HSM actions with success or failure status.
func (r *TestRequest) End(offset, length uint64, flags int, errval int) error {
	r.handleProgressReceived <- &TestProgressUpdate{
		Cookie:   r.cookie,
		Offset:   offset,
		Length:   length,
		Flags:    flags,
		Total:    length,
		Errval:   errval,
		Complete: true,
	}
	close(r.handleProgressReceived)
	return nil
}

// Fid returns the FID of the test file
func (r *TestRequest) Fid() *lustre.Fid {
	return r.testFid
}

// Cookie returns intneral request identifier
func (r *TestRequest) Cookie() uint64 {
	return r.cookie
}

// DataFid is the FID of the file used to restore data.
func (r *TestRequest) DataFid() (*lustre.Fid, error) {
	return r.testFid, nil
}

// Fd is the file descriptor for restore file.
func (r *TestRequest) Fd() (int, error) {
	return 0, nil
}

// Offset is the starting offset for a data transfer.
func (r *TestRequest) Offset() uint64 {
	return 0
}

// Length is lenght of data transfer that begins at Offset.
func (r *TestRequest) Length() uint64 {
	return 0
}

// Data is extra data that may have been provided through the HSM_REQUEST API.
func (r *TestRequest) Data() []byte {
	return r.data
}
