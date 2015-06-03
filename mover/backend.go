package main

import (
	"fmt"
	"time"

	"github.intel.com/hpdd/policy/pdm"

	"github.com/golang/glog"
)

type (

	// Backend , by convention, implments at least one of the following interfaces.
	Backend interface{}

	// Archiver allows file data to be stored on the backend with the Archive method.
	Archiver interface {
		Archive(*pdm.Request) *pdm.Result
	}

	// Restorer allows file data to be retrieved from backend with Restore method.
	Restorer interface {
		Restore(*pdm.Request) *pdm.Result
	}

	// Remover deletes file contents from the backend.
	Remover interface {
		Remove(*pdm.Request) *pdm.Result
	}

	// Canceler aborts a file stransfer.
	Canceler interface {
		Cancel(*pdm.Request) *pdm.Result
	}
)

// func (r result) Error() error {
// 	return r.err
// }

// func (r result) ResultCode() int {
// 	return r.rc
// }

// func (r result) Offset() uint64 {
// 	return r.offset
// }

// func (r result) Length() uint64 {
// 	return r.length
// }

func handleAction(backend Backend, r *pdm.Request) (*pdm.Result, error) {
	glog.Infof("%v Start\n", r)
	// In case we fall through one of the case statements
	res := ErrorResult(fmt.Errorf("Action %s not supported by %s", r.Command, backend), -1)

	start := time.Now()
	switch r.Command {
	case pdm.ArchiveCommand:
		if archiver, ok := backend.(Archiver); ok {
			glog.V(3).Infoln("calling Archive")
			res = archiver.Archive(r)
		}
	case pdm.RestoreCommand:
		if restorer, ok := backend.(Restorer); ok {
			glog.V(3).Infoln("calling Restore")
			res = restorer.Restore(r)
		}
	case pdm.RemoveCommand:
		if remover, ok := backend.(Remover); ok {
			glog.V(3).Infoln("calling Remove")
			res = remover.Remove(r)
		}
	case pdm.CancelCommand:
		if canceler, ok := backend.(Canceler); ok {
			res = canceler.Cancel(r)
		}
	default:
		res = ErrorResult(fmt.Errorf("%s oops", r.Command), -1)
	}
	elapsed := time.Since(start)

	if res.ErrorCode != 0 {
		glog.Infof("%v FAIL %s %d %v\n", r, res.Error, res.ErrorCode, elapsed)
	} else {
		glog.Infof("%v SUCCESS [%d, %d] %v\n", r, res.Offset, res.Length, elapsed)
	}

	res.Agent = r.Agent
	res.Cookie = r.Cookie
	return res, nil

}

func ErrorResult(err error, rc int) *pdm.Result {
	return &pdm.Result{Status: "FAIL", Error: err.Error(), ErrorCode: rc}
}

func GoodResult(offset, length uint64) *pdm.Result {
	return &pdm.Result{Status: "DONE", Offset: offset, Length: length}
}
