// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package simulator

import (
	"fmt"
	"io"
	"time"

	"github.com/rcrowley/go-metrics"
	"github.com/intel-hpdd/go-lustre"
	"github.com/intel-hpdd/go-lustre/changelog"
)

type (
	recordChannel chan changelog.Record
	doneChannel   chan struct{}

	simulatorOption func(*Simulator) error

	// Simulator implements a changelog simulator
	Simulator struct {
		start          time.Time
		end            time.Time
		done           doneChannel
		recordQueue    recordChannel
		indexGenerator <-chan int64
		fidGenerator   <-chan *lustre.Fid
		jobs           map[string]*simJob
	}
)

// Stats returns a string of simulator stats
func (s *Simulator) Stats() string {
	elapsed := s.end.Sub(s.start)
	lastIndex := <-s.indexGenerator - 1
	seconds := float64(elapsed) / 1e9
	return fmt.Sprintf("Generated %d changelog records in %s (%.02f/sec)\n", lastIndex, elapsed, float64(lastIndex)/seconds)
}

// GetHandle returns a changelog.Handle
func (s *Simulator) GetHandle() changelog.Handle {
	return &simHandle{
		sim: s,
	}
}

// AddJob creates a new job and adds it to the simulator
func (s *Simulator) AddJob(options ...simJobOption) error {
	job, err := newJob(s.fidGenerator, options...)
	if err != nil {
		return err
	}
	if _, ok := s.jobs[job.id]; ok {
		return fmt.Errorf("Job with id %s already exists!", job.id)
	}
	s.jobs[job.id] = job

	return nil
}

// Start indicates that the simulator should start collecting records
func (s *Simulator) Start() {
	if len(s.jobs) < 1 {
		panic("Start() called with no jobs!")
	}
	recordQueueDepth := metrics.NewGauge()
	metrics.Register("sim-recordQueue-depth", recordQueueDepth)

	jobRecordDepths := make(map[string]metrics.Gauge)
	for id := range s.jobs {
		jobRecordDepths[id] = metrics.NewGauge()
		metrics.Register(fmt.Sprintf("%s-records-depth", id), jobRecordDepths[id])
	}

	for id, job := range s.jobs {
		go func(id string, job *simJob) {
			for rec := range job.records {
				s.recordQueue <- rec
			}
			close(job.done)
			return
		}(id, job)
	}

	go func() {
		for {
			for id, job := range s.jobs {
				jobRecordDepths[id].Update(int64(len(job.records)))
				select {
				case <-job.done:
					delete(s.jobs, id)
				}
			}
			recordQueueDepth.Update(int64(len(s.recordQueue)))
			if len(s.jobs) < 1 {
				close(s.recordQueue)
				return
			}
			// This only needs to run every 1ms
			time.Sleep(1e6 * time.Nanosecond)
		}
	}()
}

// Stop indicates that the simulator should shut down
func (s *Simulator) Stop() {
	close(s.done)
	s.end = time.Now()
}

// NextRecord returns the next simulated record
func (s *Simulator) NextRecord() (changelog.Record, error) {
	select {
	case rec := <-s.recordQueue:
		if r, ok := rec.(*simRecord); ok {
			r.index = <-s.indexGenerator
			return r, nil
		}
	}
	return nil, io.EOF
}

func newIndexGenerator() <-chan int64 {
	index := int64(1)
	nextIndex := make(chan int64)

	go func() {
		for {
			// Interesting to watch this to see when the GC
			// pauses, sometimes for seconds at a time...
			//fmt.Fprintf(os.Stderr, "%d\r", index)
			select {
			case nextIndex <- index:
				index++
			}
		}
	}()

	return nextIndex
}

func newFidGenerator() <-chan *lustre.Fid {
	seq := uint64(1)
	oid := uint32(0)
	ver := uint32(1)
	fids := make(chan *lustre.Fid)

	go func() {
		for {
			nextFid := &lustre.Fid{
				Seq: seq,
				Oid: oid,
				Ver: ver,
			}
			select {
			case fids <- nextFid:
				seq++
			}
		}
	}()

	return fids
}

// New returns a newly-initialized simulator
func New(options ...simulatorOption) (*Simulator, error) {
	sim := &Simulator{
		start:          time.Now(),
		done:           make(doneChannel),
		fidGenerator:   newFidGenerator(),
		indexGenerator: newIndexGenerator(),
		jobs:           make(map[string]*simJob),
		recordQueue:    make(recordChannel, 1024),
	}

	for _, option := range options {
		if err := option(sim); err != nil {
			return nil, err
		}
	}

	return sim, nil
}
