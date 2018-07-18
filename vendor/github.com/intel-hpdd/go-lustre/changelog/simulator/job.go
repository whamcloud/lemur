// Copyright (c) 2016 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package simulator

import (
	"strconv"
	"time"

	"github.com/intel-hpdd/go-lustre"
)

type (
	simJobOption func(*simJob) error

	simJobFile struct {
		name   string
		fid    *lustre.Fid
		parent *lustre.Fid
	}

	simJob struct {
		id                   string
		maxFileCount         int
		minFileCount         int
		maxFilesPerDirectory int
		minFilesPerDirectory int
		maxFileSize          int64
		minFileSize          int64

		fidGenerator <-chan *lustre.Fid
		records      recordChannel
		files        []*simJobFile
		done         doneChannel
	}
)

func (j *simJob) createFiles() {
	lastParent := &lustre.Fid{} // zero fid
	for i := 0; i < j.maxFileCount; i++ {
		file := &simJobFile{
			name:   strconv.Itoa(i),
			parent: lastParent,
		}
		file.fid = <-j.fidGenerator
		j.sendCreateRecord(file)
		if i%j.maxFilesPerDirectory == 0 {
			lastParent = file.fid
		}
		j.files[i] = file
	}
}

func (j *simJob) deleteFiles() {
	for _, file := range j.files {
		j.sendUnlinkRecord(file)
	}
}

func (j *simJob) sendCreateRecord(file *simJobFile) {
	rec := &simRecord{
		name:       file.name,
		typeString: "CREAT",
		typeCode:   1,
		time:       time.Now(),
		targetFid:  file.fid,
		parentFid:  file.parent,
		jobID:      j.id,
	}
	j.records <- rec
}

func (j *simJob) sendUnlinkRecord(file *simJobFile) {
	rec := &simRecord{
		name:       file.name,
		typeString: "UNLNK",
		typeCode:   6,
		time:       time.Now(),
		targetFid:  file.fid,
		parentFid:  file.parent,
		jobID:      j.id,
	}
	j.records <- rec
}

func (j *simJob) Start() {
	go func() {
		j.createFiles()
		j.deleteFiles()
		close(j.records)
	}()
}

// OptJobMaxFileCount sets the maximum file count for the job
func OptJobMaxFileCount(count int) func(*simJob) error {
	return func(j *simJob) error {
		j.maxFileCount = count
		return nil
	}
}

// OptJobMinFileCount sets the minimum file count for the job
func OptJobMinFileCount(count int) func(*simJob) error {
	return func(j *simJob) error {
		j.minFileCount = count
		return nil
	}
}

// OptJobMaxFilesPerDirectory sets the maximum file count per directory for the job
func OptJobMaxFilesPerDirectory(count int) func(*simJob) error {
	return func(j *simJob) error {
		j.maxFilesPerDirectory = count
		return nil
	}
}

// OptJobMinFilesPerDirectory sets the minimum file count per directory for the job
func OptJobMinFilesPerDirectory(count int) func(*simJob) error {
	return func(j *simJob) error {
		j.minFilesPerDirectory = count
		return nil
	}
}

// OptJobMaxFileSize sets the maximum file size for the job
func OptJobMaxFileSize(size int64) func(*simJob) error {
	return func(j *simJob) error {
		j.maxFileSize = size
		return nil
	}
}

// OptJobMinFileSize sets the minimum file size for the job
func OptJobMinFileSize(size int64) func(*simJob) error {
	return func(j *simJob) error {
		j.minFileSize = size
		return nil
	}
}

// OptJobID sets the job id
func OptJobID(id string) func(*simJob) error {
	return func(j *simJob) error {
		j.id = id
		return nil
	}
}

func newJob(fids <-chan *lustre.Fid, options ...simJobOption) (*simJob, error) {
	job := &simJob{
		// set some defaults
		id:                   "sim-job",
		maxFileCount:         4096,
		maxFilesPerDirectory: 512,

		fidGenerator: fids,
		done:         make(doneChannel),
	}

	for _, option := range options {
		if err := option(job); err != nil {
			return nil, err
		}
	}
	job.files = make([]*simJobFile, job.maxFileCount)
	job.records = make(recordChannel, 1024)

	job.Start()
	return job, nil
}
