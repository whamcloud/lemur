// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package agent

import (
	"errors"
	"sync"
	"sync/atomic"
)

type (
	// Handle is an endpoint handle (unique id)
	Handle uint64

	// Endpoints represents a collection of Endpoints and their handles
	Endpoints struct {
		sync.Mutex
		nextHandle int64
		endpoints  map[uint32]Endpoint
		handles    map[Handle]uint32
	}

	// Endpoint defines an interface for HSM backends
	Endpoint interface {
		Send(*Action)
	}
)

// NewEndpoints returns a new *Endpoints instance
func NewEndpoints() *Endpoints {
	return &Endpoints{
		endpoints: make(map[uint32]Endpoint),
		handles:   make(map[Handle]uint32),
	}
}

// Get returns an Endpoint or nil, given a lookup id
func (all *Endpoints) Get(a uint32) (Endpoint, bool) {
	all.Lock()
	defer all.Unlock()
	return all.get(a)
}

// GetWithHandle returns an Endpoint or nil, given a Handle
func (all *Endpoints) GetWithHandle(h *Handle) (Endpoint, bool) {
	all.Lock()
	defer all.Unlock()
	return all.getWithHandle(h)
}

func (all *Endpoints) get(a uint32) (Endpoint, bool) {
	// all must already be locked.
	e, ok := all.endpoints[a]
	if !ok {
		return nil, ok
	}
	return e, true
}

func (all *Endpoints) getWithHandle(h *Handle) (Endpoint, bool) {
	// all must already be locked.
	a, ok := all.handles[*h]
	if !ok {
		return nil, ok
	}

	return all.get(a)
}

func (all *Endpoints) newHandle() *Handle {
	h := Handle(atomic.AddInt64(&all.nextHandle, 1))
	return &h
}

// Add registers a new Endpoint
func (all *Endpoints) Add(a uint32, e Endpoint) (*Handle, error) {
	h := all.newHandle()
	all.Lock()
	defer all.Unlock()

	if _, ok := all.get(a); ok {
		return nil, errors.New("Endpoint already exists")
	}

	all.endpoints[a] = e
	all.handles[*h] = a
	return h, nil
}

// NewHandle returns a new *Handle
func (all *Endpoints) NewHandle(a uint32) (*Handle, error) {
	all.Lock()
	defer all.Unlock()

	if _, ok := all.get(a); !ok {
		return nil, errors.New("Endpoint does not exist")
	}

	h := all.newHandle()
	all.handles[*h] = a
	return h, nil

}

// RemoveHandle removes the given handle from the collection of handles
func (all *Endpoints) RemoveHandle(h *Handle) {
	all.Lock()
	defer all.Unlock()

	delete(all.handles, *h)
}

// Remove removes the given handle and its associated Endpoint
func (all *Endpoints) Remove(h *Handle) Endpoint {
	all.Lock()
	defer all.Unlock()
	a, ok := all.handles[*h]
	if !ok {
		return nil
	}

	if e, ok := all.get(a); ok {
		delete(all.handles, *h)
		delete(all.endpoints, a)
		return e
	}

	return nil
}
