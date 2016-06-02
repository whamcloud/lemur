package agent

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.intel.com/hpdd/lemur/cmd/lhsmd/agent/fileid"
	pb "github.intel.com/hpdd/lemur/pdm"
	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/audit"
	"github.intel.com/hpdd/logging/debug"

	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/hsm"
	"github.intel.com/hpdd/lustre/llapi"
)

type (
	// ActionID is a unique (per agent instance) ID for HSM actions
	ActionID uint64

	// Action represents an HSM action
	Action struct {
		id     ActionID
		aih    hsm.ActionHandle
		agent  *HsmAgent
		start  time.Time
		FileID []byte
		Data   []byte
	}

	// ActionData is extra data passed to the Agent by policy engine
	ActionData struct {
		FileID    []byte `json:"file_id"`
		MoverData []byte `json:"mover_data"`
	}
)

var actionIDCounter ActionID

// NextActionID returns monotonically-increasing ActionIDs
func NextActionID() ActionID {
	return ActionID(atomic.AddUint64((*uint64)(&actionIDCounter), 1))
}

func (action *Action) String() string {
	return fmt.Sprintf("id:%d %s %v ", action.id, action.aih.Action(), action.aih.Fid())
}

func hsm2Command(a llapi.HsmAction) (c pb.Command) {
	switch a {
	case llapi.HsmActionArchive:
		c = pb.Command_ARCHIVE
	case llapi.HsmActionRestore:
		c = pb.Command_RESTORE
	case llapi.HsmActionRemove:
		c = pb.Command_REMOVE
	case llapi.HsmActionCancel:
		c = pb.Command_CANCEL
	default:
		alert.Abort(errors.Errorf("unknown command: %v", a))
	}

	return
}

// Handle returns the raw hsm.ActionHandle (temporary function until queue
// transport is updated)
func (action *Action) Handle() hsm.ActionHandle {
	return action.aih
}

// ID Returns the action id.
func (action *Action) ID() ActionID {
	return action.id
}

// Prepare ensure action is ready to be sent.
// Complete any actions that may require accessing the filesystem.
func (action *Action) Prepare() error {
	var data ActionData
	if len(action.aih.Data()) > 0 {
		err := json.Unmarshal(action.aih.Data(), &data)
		if err != nil {
			alert.Warnf("unrecognized data passed to agent: %v: %v", action.aih.Data(), err)
		}
	}

	if len(data.MoverData) > 0 {
		action.Data = data.MoverData
	}

	if len(data.FileID) > 0 {
		debug.Printf("found fileID from user: %v %d", data.FileID, len(data.FileID))
		action.FileID = data.FileID
	} else {
		switch action.aih.Action() {
		case llapi.HsmActionRestore, llapi.HsmActionRemove:
			var err error
			action.FileID, err = fileid.Get(action.agent.Root(), action.aih.Fid())
			if err != nil {
				debug.Printf("Error reading fileid: %v (%v) will retry", err, action)
				// WTF, let's try again
				time.Sleep(1 * time.Second)
				action.FileID, err = fileid.Get(action.agent.Root(), action.aih.Fid())

			}
			if err != nil {
				alert.Warnf("Error reading fileid: %v (%v)", err, action) // hmm, can't restore if there is no file id
			}
		}
	}
	return nil
}

// AsMessage returns the protobuf version of an Action.
func (action *Action) AsMessage() *pb.ActionItem {
	msg := &pb.ActionItem{
		Id:          uint64(action.id),
		Op:          hsm2Command(action.aih.Action()),
		PrimaryPath: fs.FidRelativePath(action.aih.Fid()),
		Offset:      action.aih.Offset(),
		Length:      action.aih.Length(),
		FileId:      action.FileID,
		Data:        action.Data,
	}

	dfid, err := action.aih.DataFid()
	if err == nil {
		msg.WritePath = fs.FidRelativePath(dfid)
	}

	return msg
}

// Update handles the Status messages from the data mover. The Status
// updates the current progress of the Action. if the Completed flag is true,
// then the Action is completed and true is returned so the transport can remove
// any related state. After an action is completed any further status updates
// should be ignored.
//
// If this function returns an error then the transport layer should notify
// the mover that this action has been terminated. In this case the Action will
// be completed immediately and no further updates are required.
//
func (action *Action) Update(status *pb.ActionStatus) (bool, error) {
	debug.Printf("id:%d update offset: %d length: %d complete: %v status: %d", status.Id,
		status.Offset,
		status.Length,
		status.Completed, status.Error)
	if status.Completed {
		duration := time.Since(action.start)
		debug.Printf("id:%d completed status: %v in %v", status.Id, status.Error, duration)

		if status.FileId != nil {
			fileid.Update(action.agent.Root(), action.aih.Fid(), status.FileId)
		}
		action.agent.stats.CompleteAction(action, int(status.Error))
		err := action.aih.End(status.Offset, status.Length, 0, int(status.Error))
		if err != nil {
			audit.Logf("id:%d completion failed: %v", status.Id, err)
			return true, err // Completed, but Failed. Internal HSM state is not updated
		}

		if action.aih.Action() == llapi.HsmActionArchive && action.agent.config.Snapshots.Enabled && status.FileId != nil {
			createSnapshot(action.agent.Root(), action.aih.ArchiveID(), action.aih.Fid(), status.FileId)
		}
		return true, nil // Completed
	}
	err := action.aih.Progress(status.Offset, status.Length, action.aih.Length(), 0)
	if err != nil {
		debug.Printf("id:%d progress update failed: %v", status.Id, err)
		action.agent.stats.CompleteAction(action, -1)
		if err2 := action.aih.End(0, 0, 0, -1); err2 != nil {
			debug.Printf("id:%d completion after error failed: %v", status.Id, err2)
			return false, fmt.Errorf("err: %s/err2: %s", err, err2)
		}
		return false, err // Incomplete Failed Action
	}

	return false, nil
}

// Fail signals that the action has failed
func (action *Action) Fail(rc int) error {
	audit.Logf("id:%d fail %x %v: %v", action.id, action.aih.Cookie, action.aih.Fid(), rc)
	action.agent.stats.CompleteAction(action, rc)
	err := action.aih.End(0, 0, 0, rc)
	if err != nil {
		audit.Logf("id:%d fail after fail %x: %v", action.id, action.aih.Cookie, err)
	}
	return errors.Wrap(err, "end action failed")

}
