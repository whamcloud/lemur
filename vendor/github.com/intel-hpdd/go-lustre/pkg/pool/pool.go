// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package pool

import (
	"errors"
	"sync"
	"sync/atomic"
)

// Pool is a pool of resources with a given allocator. If the a resource implements Closer, then
// it  Close() will be called when resource is removed from the pool.
type Pool struct {
	mu        sync.Mutex // protects allocated
	min       int
	max       int
	allocated int
	resources chan interface{}
	alloc     func() (interface{}, error)

	closed uint32
}

// Closer interface implements Close().
type Closer interface {
	Close() error
}

// ErrClosed indicates this pool is already closed.
var ErrClosed = errors.New("Pool is closed")

// New returns a new, initialized Pool.
func New(name string, min, max int, alloc func() (interface{}, error)) (*Pool, error) {
	p := &Pool{
		min:       min,
		max:       max,
		alloc:     alloc,
		resources: make(chan interface{}, max),
	}
	if p.min > p.max {
		p.min = p.max
	}

	for p.allocated < p.min {
		res, err := p.alloc()
		if err != nil {
			p.Close()
			return nil, err
		}
		p.allocated++
		p.resources <- res
	}
	return p, nil
}

// Allocated returns current count of resources being managed by the pool.
func (p *Pool) Allocated() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.allocated
}

// Get returns a resource or err if pool is closed. This function will
// block until a resource is available.
func (p *Pool) Get() (interface{}, error) {
	if atomic.LoadUint32(&p.closed) == 1 {
		return nil, ErrClosed
	}
	select {
	case res := <-p.resources:
		return res, nil
	default:
		return p.addOrWait()
	}
}

// Put places a resource back into the Pool.
func (p *Pool) Put(res interface{}) {
	if atomic.LoadUint32(&p.closed) == 1 {
		p.deleteRes(res)
		return
	}

	select {
	case p.resources <- res:
	default:
		p.deleteRes(res)
	}
}

func (p *Pool) addOrWait() (interface{}, error) {
	if atomic.LoadUint32(&p.closed) == 1 {
		return nil, ErrClosed
	}
	p.mu.Lock()
	if p.allocated < p.max {
		res, err := p.alloc()
		if err != nil {
			return nil, err
		}
		p.allocated++
		p.resources <- res
	}
	p.mu.Unlock()
	return <-p.resources, nil
}

func (p *Pool) deleteRes(o interface{}) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.allocated--
	c, ok := o.(Closer)
	if ok {
		return c.Close()
	}
	return nil
}

// Close removes all resources from the pool.
func (p *Pool) Close() (err error) {
	atomic.StoreUint32(&p.closed, 1)
	for {
		if p.Allocated() == 0 {
			return
		}
		select {
		case res := <-p.resources:
			p.deleteRes(res)
		default:
		}
	}
}
