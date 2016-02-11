package agent

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/audit"
	"github.intel.com/hpdd/logging/debug"
	pb "github.intel.com/hpdd/policy/pdm/pdm"

	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/hsm"
	"github.intel.com/hpdd/lustre/llapi"
)

// ActionID is a unique (per agent instance) ID for HSM actions
type ActionID uint64

var actionIDCounter ActionID

// NextActionID returns monotonically-increasing ActionIDs
func NextActionID() ActionID {
	return ActionID(atomic.AddUint64((*uint64)(&actionIDCounter), 1))
}

// Action represents an HSM action
type Action struct {
	id    ActionID
	aih   hsm.ActionHandle
	agent *HsmAgent
	start time.Time
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
		alert.Fatalf("unknown command: %v", a)
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

// AsMessage returns the protobuf version of an Action.
func (action *Action) AsMessage() *pb.ActionItem {
	msg := &pb.ActionItem{
		Id:          uint64(action.id),
		Op:          hsm2Command(action.aih.Action()),
		PrimaryPath: fs.FidRelativePath(action.aih.Fid()),
		Offset:      action.aih.Offset(),
		Length:      action.aih.Length(),
		Data:        action.aih.Data(),
	}

	switch action.aih.Action() {
	case llapi.HsmActionRestore, llapi.HsmActionRemove:
		var err error
		msg.FileId, err = getFileID(action.agent.Root(), action.aih.Fid())
		if err != nil {
			log.Println(err) //hmm, can't restore if there is no file id
		}
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
		// s.stats.Latencies.Update(duration.Nanoseconds())

		if status.FileId != nil {
			updateFileID(action.agent.Root(), action.aih.Fid(), status.FileId)
		}
		err := action.aih.End(status.Offset, status.Length, 0, int(status.Error))
		if err != nil {
			audit.Logf("id:%d completion failed: %v", status.Id, err)
			return true, err // Completed, but Failed. Internal HSM state is not updated
		}
		return true, nil // Completed
	}
	err := action.aih.Progress(status.Offset, status.Length, action.aih.Length(), 0)
	if err != nil {
		debug.Printf("id:%d progress update failed: %v", status.Id, err)

		if err2 := action.aih.End(0, 0, 0, -1); err2 != nil {
			debug.Printf("id:%d completion after error failed: %v", status.Id, err2)
			return false, fmt.Errorf("%s/%s", err, err2)
		}
		return false, err // Incomplete Failed Action
	}

	return false, nil
}

// Fail signals that the action has failed
func (action *Action) Fail(rc int) error {
	audit.Logf("id:%d fail %x %v: %v", action.id, action.aih.Cookie, action.aih.Fid(), rc)
	err := action.aih.End(0, 0, 0, -1)
	if err != nil {
		audit.Logf("id:%d fail after fail %x: %v", action.id, action.aih.Cookie, err)
	}
	return err

}
