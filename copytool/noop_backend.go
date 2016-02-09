package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/hsm"
)

type (
	NoopBackend struct {
		root fs.RootDir
	}
)

func NewNoopBackend(root fs.RootDir) *NoopBackend {
	return &NoopBackend{root: root}
}

func (back NoopBackend) String() string {
	return fmt.Sprintf("Noop backend for %s\n", back.root)
}

func (back NoopBackend) Archive(aih hsm.ActionHandle) ActionResult {
	names, err := fs.FidPathnames(back.root, aih.Fid())
	if err != nil {
		// ?
	}
	debug.Printf("%v %v", aih, names)
	time.Sleep((time.Duration(rand.Intn(3)) + 1) * time.Second)
	return ErrorResult(fmt.Errorf("unsupported"), -1)
}

func (back NoopBackend) Remove(aih hsm.ActionHandle) ActionResult {
	names, err := fs.FidPathnames(back.root, aih.Fid())
	if err != nil {
		// ?
	}
	debug.Printf("%v %v", aih, names)
	time.Sleep((time.Duration(rand.Intn(3)) + 1) * time.Second)
	return ErrorResult(fmt.Errorf("unsupported"), -1)
}
