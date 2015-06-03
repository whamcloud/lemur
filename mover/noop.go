package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/policy/pdm"
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

func (back NoopBackend) Archive(aih *pdm.Request) *pdm.Result {
	log.Printf("%v \n", aih)
	time.Sleep((time.Duration(rand.Intn(3)) + 1) * time.Second)
	return ErrorResult(fmt.Errorf("unsupported"), -1)
}

func (back NoopBackend) Remove(aih *pdm.Request) *pdm.Result {
	log.Printf("%v \n", aih)
	time.Sleep((time.Duration(rand.Intn(3)) + 1) * time.Second)
	return ErrorResult(fmt.Errorf("unsupported"), -1)
}
