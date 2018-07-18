// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"net/url"
	"path"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pkg/errors"

	"github.com/intel-hpdd/lemur/dmplugin"
	"github.com/intel-hpdd/lemur/dmplugin/dmio"
	"github.com/intel-hpdd/logging/debug"
	"github.com/pborman/uuid"
)

// Mover is an S3 data mover
type Mover struct {
	name  string
	s3Svc *s3.S3
	cfg   *archiveConfig
}

// S3Mover returns a new *Mover
func S3Mover(cfg *archiveConfig, s3Svc *s3.S3, archiveID uint32) *Mover {
	return &Mover{
		name:  fmt.Sprintf("s3-%d", archiveID),
		s3Svc: s3Svc,
		cfg:   cfg,
	}
}

func newFileID() string {
	return uuid.New()
}

func (m *Mover) destination(id string) string {
	return path.Join(m.cfg.Prefix,
		"o",
		id)
}

func (m *Mover) newUploader() *s3manager.Uploader {
	// can configure stuff here with custom setters
	var partSize = func(u *s3manager.Uploader) {
		u.PartSize = m.cfg.UploadPartSize
	}
	return s3manager.NewUploaderWithClient(m.s3Svc, partSize)

}

func (m *Mover) newDownloader() *s3manager.Downloader {
	return s3manager.NewDownloaderWithClient(m.s3Svc)
}

// Start signals the mover to begin any asynchronous processing (e.g. stats)
func (m *Mover) Start() {
	debug.Printf("%s started", m.name)
}
func (m *Mover) fileIDtoBucketPath(fileID string) (string, string, error) {
	var bucket, path string

	u, err := url.ParseRequestURI(fileID)
	if err == nil {
		if u.Scheme != "s3" {
			return "", "", errors.Errorf("invalid URL in file_id %s", fileID)
		}
		path = u.Path
		bucket = u.Host
	} else {
		path = m.destination(fileID)
		bucket = m.cfg.Bucket
	}
	debug.Printf("Parsed %s -> %s/%s", fileID, bucket, path)
	return bucket, path, nil
}

// Archive fulfills an HSM Archive request
func (m *Mover) Archive(action dmplugin.Action) error {
	debug.Printf("%s id:%d archive %s %s", m.name, action.ID(), action.PrimaryPath(), action.UUID())
	rate.Mark(1)
	start := time.Now()

	fileID := newFileID()
	fileKey := m.destination(fileID)

	rdr, total, err := dmio.NewActionReader(action)
	if err != nil {
		return errors.Wrapf(err, "Could not create archive reader for %s", action)
	}
	defer rdr.Close()

	progressFunc := func(offset, length int64) error {
		return action.Update(offset, length, total)
	}
	progressReader := dmio.NewProgressReader(rdr, updateInterval, progressFunc)
	defer progressReader.StopUpdates()

	uploader := m.newUploader()
	out, err := uploader.Upload(&s3manager.UploadInput{
		Body:        progressReader,
		Bucket:      aws.String(m.cfg.Bucket),
		Key:         aws.String(fileKey),
		ContentType: aws.String("application/octet-stream"),
	})
	if err != nil {
		if multierr, ok := err.(s3manager.MultiUploadFailure); ok {
			return errors.Errorf("Upload error on %s: %s (%s)", multierr.UploadID(), multierr.Code(), multierr.Message())
		}
		return errors.Wrap(err, "upload failed")
	}

	debug.Printf("%s id:%d Archived %d bytes in %v from %s to %s", m.name, action.ID(), total,
		time.Since(start),
		action.PrimaryPath(),
		out.Location)

	u := url.URL{
		Scheme: "s3",
		Host:   m.cfg.Bucket,
		Path:   fileKey,
	}

	action.SetUUID(fileID)
	action.SetURL(u.String())
	action.SetActualLength(total)
	return nil
}

// Restore fulfills an HSM Restore request
func (m *Mover) Restore(action dmplugin.Action) error {
	debug.Printf("%s id:%d restore %s %s", m.name, action.ID(), action.PrimaryPath(), action.UUID())
	rate.Mark(1)

	start := time.Now()
	if action.UUID() == "" {
		return errors.Errorf("Missing file_id on action %d", action.ID())
	}
	bucket, srcObj, err := m.fileIDtoBucketPath(action.UUID())
	if err != nil {
		return errors.Wrap(err, "fileIDtoBucketPath failed")
	}
	out, err := m.s3Svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(srcObj),
	})

	if err != nil {
		return errors.Wrapf(err, "s3.HeadObject() on %s failed", srcObj)
	}
	debug.Printf("obj %s, size %d", srcObj, *out.ContentLength)

	dstSize := *out.ContentLength
	dst, err := dmio.NewActionWriter(action)
	if err != nil {
		return errors.Wrapf(err, "Couldn't create ActionWriter for %s", action)
	}
	defer dst.Close()

	progressFunc := func(offset, length int64) error {
		return action.Update(offset, length, dstSize)
	}
	progressWriter := dmio.NewProgressWriterAt(dst, updateInterval, progressFunc)
	defer progressWriter.StopUpdates()

	downloader := m.newDownloader()
	n, err := downloader.Download(progressWriter,
		&s3.GetObjectInput{
			Bucket: aws.String(m.cfg.Bucket),
			Key:    aws.String(srcObj),
		})
	if err != nil {
		return errors.Errorf("s3.Download() of %s failed: %s", srcObj, err)
	}

	debug.Printf("%s id:%d Restored %d bytes in %v from %s to %s", m.name, action.ID(), n,
		time.Since(start),
		srcObj,
		action.PrimaryPath())
	action.SetActualLength(n)
	return nil
}

// Remove fulfills an HSM Remove request
func (m *Mover) Remove(action dmplugin.Action) error {
	debug.Printf("%s id:%d remove %s %s", m.name, action.ID(), action.PrimaryPath(), action.UUID())
	rate.Mark(1)
	if action.UUID() == "" {
		return errors.New("Missing file_id")
	}

	bucket, srcObj, err := m.fileIDtoBucketPath(string(action.UUID()))

	_, err = m.s3Svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(srcObj),
	})
	return errors.Wrap(err, "delete object failed")
}
