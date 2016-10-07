// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package hsm

import (
	"errors"
	"fmt"
	"sync"

	"github.com/intel-hpdd/logging/alert"
	"github.com/intel-hpdd/go-lustre"
	"github.com/intel-hpdd/go-lustre/fs"
	"github.com/intel-hpdd/go-lustre/llapi"
	"github.com/intel-hpdd/go-lustre/status"
	"golang.org/x/sys/unix"
)

// Expose the internal constants for external users
const (
	NONE    = llapi.HsmActionNone
	ARCHIVE = llapi.HsmActionArchive
	RESTORE = llapi.HsmActionRestore
	REMOVE  = llapi.HsmActionRemove
	CANCEL  = llapi.HsmActionCancel
)

type (
	// CoordinatorClient receives HSM actions to execute.
	CoordinatorClient struct {
		root fs.RootDir
		hcp  *llapi.HsmCopytoolPrivate
	}

	// ActionItem is one action to perform on specified file.
	actionItem struct {
		mu        sync.Mutex
		cdc       *CoordinatorClient
		hcap      *llapi.HsmCopyActionPrivate
		hai       llapi.HsmActionItem
		halFlags  uint64
		archiveID uint
	}

	// ErrIOError are errors that returned by the HSM library.
	ErrIOError struct {
		msg string
	}
)

func (e ErrIOError) Error() string {
	return e.msg
}

// IoError returns a new error.
func IoError(msg string) error {
	return errors.New(msg)
}

// NewCoordinatorClient opens a connection to the coordinator.
func NewCoordinatorClient(path fs.RootDir, nonBlocking bool) (*CoordinatorClient, error) {
	var cdc = CoordinatorClient{root: path}
	var err error

	flags := llapi.CopytoolDefault

	if nonBlocking {
		flags = llapi.CopytoolNonBlock
	}

	cdc.hcp, err = llapi.HsmCopytoolRegister(path.String(), 0, nil, flags)
	if err != nil {
		return nil, err
	}
	return &cdc, nil
}

// Recv blocks and waits for new action items from the coordinator.
// Retuns a slice of *actionItem.
func (cdc *CoordinatorClient) recv() ([]*actionItem, error) {

	if cdc.hcp == nil {
		return nil, errors.New("coordinator closed")
	}
	actionList, err := llapi.HsmCopytoolRecv(cdc.hcp)
	if err != nil {
		return nil, err
	}
	items := make([]*actionItem, len(actionList.Items))
	for i, hai := range actionList.Items {
		item := &actionItem{
			halFlags:  actionList.Flags,
			archiveID: actionList.ArchiveID,
			cdc:       cdc,
			hai:       hai,
		}
		items[i] = item
	}
	return items, nil
}

//GetFd returns copytool file descriptor
func (cdc *CoordinatorClient) GetFd() int {
	return llapi.HsmCopytoolGetFd(cdc.hcp)
}

// Close terminates connection with coordinator.
func (cdc *CoordinatorClient) Close() {
	if cdc.hcp != nil {
		llapi.HsmCopytoolUnregister(&cdc.hcp)
		cdc.hcp = nil
	}
}

type (
	// ActionRequest is an HSM action
	ActionRequest interface {
		Begin(openFlags int, isError bool) (ActionHandle, error)
		FailImmediately(errval int)
		ArchiveID() uint
		String() string
		Action() llapi.HsmAction
	}

	// ActionHandle is an HSM action that is currently being processed
	ActionHandle interface {
		Progress(offset uint64, length uint64, totalLength uint64, flags int) error
		End(offset uint64, length uint64, flags int, errval int) error
		Action() llapi.HsmAction
		Fid() *lustre.Fid
		Cookie() uint64
		DataFid() (*lustre.Fid, error)
		Fd() (int, error)
		Offset() uint64
		ArchiveID() uint
		Length() uint64
		String() string
		Data() []byte
	}
)

// Copy the striping info from the primary to the temporary file.
//
func (ai *actionItem) copyLovMd() error {
	fd, err := ai.Fd()
	if err != nil {
		return err
	}
	defer unix.Close(fd)

	src := fs.FidPath(ai.cdc.root, ai.Fid())
	layout, err := llapi.FileDataLayout(src)
	if err != nil {
		return err
	}

	return llapi.SetFileLayout(fd, layout)
}

// Begin prepares an actionItem for processing.
//
// returns an actionItem. The End method must be called to complete
// this action.
func (ai *actionItem) Begin(openFlags int, isError bool) (ActionHandle, error) {
	mdtIndex := -1
	setLov := false
	if ai.Action() == RESTORE && !isError {
		var err error
		mdtIndex, err = status.GetMdt(ai.cdc.root, ai.Fid())
		if err != nil {

			return nil, err
		}
		openFlags = llapi.LovDelayCreate
		setLov = true
	}
	var err error
	ai.mu.Lock()
	ai.hcap, err = llapi.HsmActionBegin(ai.cdc.hcp, &ai.hai, mdtIndex, openFlags, isError)
	ai.mu.Unlock()
	if err != nil {
		ai.mu.Lock()
		llapi.HsmActionEnd(&ai.hcap, 0, 0, 0, -1)
		ai.mu.Unlock()
		return nil, err

	}
	aih := (*actionItem)(ai)
	if setLov {
		if err := aih.copyLovMd(); err != nil {
			alert.Warn(err)
		}
	}
	return aih, nil
}

// ArchiveID returns the archive id associated with teh actionItem.
func (ai *actionItem) ArchiveID() uint {
	return ai.archiveID
}

// Action returns name of the action.
func (ai *actionItem) Action() llapi.HsmAction {
	return ai.hai.Action
}

// Fid returns the FID for the actual file for ths action.
// This fid or xattrs on this file can be used as a key with
// the HSM backend.
func (ai *actionItem) Fid() *lustre.Fid {
	return ai.hai.Fid
}

// FailImmediately completes the ActinoItem with given error.
// The passed actionItem is no longer valid when this function returns.
func (ai *actionItem) FailImmediately(errval int) {
	aih, err := ai.Begin(0, true)
	if err != nil {
		return
	}
	aih.End(0, 0, 0, errval)
}

func lengthStr(length uint64) string {
	if length == ^uint64(0) {
		return "EOF"
	}
	return fmt.Sprintf("%d", length)
}

func (ai *actionItem) String() string {
	return fmt.Sprintf("AI: %x %v %v %d,%v %v", ai.hai.Cookie, ai.Action(), ai.Fid(), ai.Offset(), lengthStr(ai.Length()), ai.Data())
}

// Progress reports current progress of an action.
func (ai *actionItem) Progress(offset uint64, length uint64, totalLength uint64, flags int) error {
	ai.mu.Lock()
	defer ai.mu.Unlock()
	return llapi.HsmActionProgress(ai.hcap, offset, length, totalLength, flags)
}

// End completes an action with specified status.
// No more requests should be made on this action after calling this.
func (ai *actionItem) End(offset uint64, length uint64, flags int, errval int) error {
	ai.mu.Lock()
	defer ai.mu.Unlock()
	return llapi.HsmActionEnd(&ai.hcap, offset, length, flags, errval)
}

// Cookie returns the action identifier.
func (ai *actionItem) Cookie() uint64 {
	return ai.hai.Cookie
}

// DataFid returns the FID of the data file.
// This file should be used for all Lustre IO for archive and restore commands.
func (ai *actionItem) DataFid() (*lustre.Fid, error) {
	ai.mu.Lock()
	defer ai.mu.Unlock()
	return llapi.HsmActionGetDataFid(ai.hcap)
}

// Fd returns the file descriptor of the DataFid.
// If used, this Fd must be closed prior to calling End.
func (ai *actionItem) Fd() (int, error) {
	ai.mu.Lock()
	defer ai.mu.Unlock()
	fd, err := llapi.HsmActionGetFd(ai.hcap)
	if err != nil {
		return 0, err
	}
	return fd, nil
}

// Offset returns the offset for the action.
func (ai *actionItem) Offset() uint64 {
	return uint64(ai.hai.Extent.Offset)
}

// Length returns the length of the action request.
func (ai *actionItem) Length() uint64 {
	return uint64(ai.hai.Extent.Length)
}

// Data returns the additional request data.
// The format of the data is agreed upon by the initiator of the HSM
// request and backend driver that is doing the work.
func (ai *actionItem) Data() []byte {
	return ai.hai.Data
}
