package main

import (
	"fmt"
	"time"

	"github.intel.com/hpdd/lustre/hsm"

	"github.com/golang/glog"
)

type (

	// Backend , by convention, implments at least one of the following interfaces.
	Backend interface{}

	// Archiver allows file data to be stored on the backend with the Archive method.
	Archiver interface {
		Archive(hsm.ActionHandle) ActionResult
	}

	// Restorer allows file data to be retrieved from backend with Restore method.
	Restorer interface {
		Restore(hsm.ActionHandle) ActionResult
	}

	// Remover deletes file contents from the backend.
	Remover interface {
		Remove(hsm.ActionHandle) ActionResult
	}

	// Canceler aborts a file stransfer.
	Canceler interface {
		Cancel(hsm.ActionHandle) ActionResult
	}

	// ActionResult encapsulates the return value of an backend action handler
	ActionResult interface {
		Error() error
		ResultCode() int
		Offset() uint64
		Length() uint64
	}

	result struct {
		err    error
		rc     int
		offset uint64
		length uint64
	}
)

func (r result) Error() error {
	return r.err
}

func (r result) ResultCode() int {
	return r.rc
}

func (r result) Offset() uint64 {
	return r.offset
}

func (r result) Length() uint64 {
	return r.length
}

func handleAction(backend Backend, ai hsm.ActionRequest) error {
	aih, err := ai.Begin(0, false)
	if err != nil {
		return fmt.Errorf("Failed to begin: %s\n", err)
	}
	glog.Infof("%v Start\n", aih)

	// In case we fall through one of the case statements
	var res ActionResult
	res = ErrorResult(fmt.Errorf("Action %s not supported by %s", aih.Action(), backend), -1)

	start := time.Now()
	switch aih.Action() {
	case hsm.ARCHIVE:
		if archiver, ok := backend.(Archiver); ok {
			glog.V(3).Infoln("calling Archive")
			res = archiver.Archive(aih)
		}
	case hsm.RESTORE:
		if restorer, ok := backend.(Restorer); ok {
			glog.V(3).Infoln("calling Restore")
			res = restorer.Restore(aih)
		}
	case hsm.REMOVE:
		if remover, ok := backend.(Remover); ok {
			glog.V(3).Infoln("calling Remove")
			res = remover.Remove(aih)
		}
	case hsm.CANCEL:
		if canceler, ok := backend.(Canceler); ok {
			res = canceler.Cancel(aih)
		}
	default:
		res = ErrorResult(fmt.Errorf("%s oops", aih.Action()), -1)
	}
	elapsed := time.Since(start)

	if res.Error() != nil {
		glog.Infof("%v FAIL %s %d %v\n", aih, res.Error(), res.ResultCode(), elapsed)
		err = aih.End(0, 0, 0, res.ResultCode())
	} else {
		glog.Infof("%v SUCCESS [%d, %d] %v\n", aih, res.Offset(), res.Length(), elapsed)
		err = aih.End(res.Offset(), res.Length(), 0, 0)
	}

	if err != nil {
		// TODO: Tell backend to cleanup failed action (like a failed archive)
		return err

	}
	return nil

}

func ErrorResult(err error, rc int) ActionResult {
	return result{err: err, rc: rc}
}

func GoodResult(offset, length uint64) ActionResult {
	return result{offset: offset, length: length}
}

func handleActions(ct *CopyTool) {
	ch := ct.Actions()
	for ai := range ch {
		glog.V(1).Infof("incoming: %s", ai)
		be, ok := ct.GetBackend(ai.ArchiveID())
		if ok {
			err := handleAction(be, ai)
			if err != nil {
				glog.Infof("! failed: %v %s", ai, err)
			}
		} else {
			glog.Infoln("Action for unknown archive: ", ai.ArchiveID())
			ai.FailImmediately(-2)
		}
		glog.V(1).Infof("complete: %s", ai)
	}
	glog.Info("done handling actions")
}
