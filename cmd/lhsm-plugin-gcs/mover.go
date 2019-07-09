// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"io"
	"net/url"
	"time"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"

	"github.com/intel-hpdd/lemur/dmplugin"
	"github.com/intel-hpdd/lemur/dmplugin/dmio"
	"github.com/intel-hpdd/logging/debug"
	"github.com/pborman/uuid"
)

// Mover is a GCS data mover
type Mover struct {
	Name      string
	Ctx       context.Context
	GcsClient *storage.Client
	Bucket    string
}

// NewMover returns a new *Mover
func GcsMover(config *archiveConfig, ctx context.Context, gcsClient *storage.Client) *Mover {
	return &Mover{
		Name:      config.Name,
		Ctx:       ctx,
		GcsClient: gcsClient,
		Bucket:    config.Bucket,
	}
}

func newFileID() string {
	return uuid.New()
}

// Destination returns the path to archived file.
// Exported for testin
func (m *Mover) destination(id string) string {
	return id
}

func (m *Mover) newWriter(object string) *storage.Writer {
	return m.GcsClient.Bucket(m.Bucket).Object(object).NewWriter(m.Ctx)
}

func (m *Mover) newReader(object string) (*storage.Reader, error) {
	return m.GcsClient.Bucket(m.Bucket).Object(object).NewReader(m.Ctx)
}

func upload(wc *storage.Writer, f io.Reader) error {

	if _, err := io.Copy(wc, f); err != nil {
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}

	return nil

}

func download(wc io.Writer, f *storage.Reader) (int64, error) {

	if _, err := io.Copy(wc, f); err != nil {
		return f.Size(), err
	}
	if err := f.Close(); err != nil {
		return f.Size(), err
	}

	return f.Size(), nil

}

// Start signals the mover to begin any asynchronous processing (e.g. stats)
func (m *Mover) Start() {
	debug.Printf("%s started", m.Name)
}

func (m *Mover) Archive(action dmplugin.Action) error {
	debug.Printf("%s id:%d ARCHIVE %s", m.Name, action.ID(), action.PrimaryPath())
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

	writer := m.newWriter(fileKey)
	upload(writer, progressReader)

	if err != nil {
		return errors.Wrap(err, "upload failed")
	}

	debug.Printf("%s id:%d Archived %d bytes in %v from %s to", m.Name, action.ID(), total,
		time.Since(start),
		action.PrimaryPath())

	u := url.URL{
		Scheme: "gs",
		Host:   m.Bucket,
		Path:   fileKey,
	}

	action.SetUUID(fileID)
	action.SetURL(u.String())
	action.SetActualLength(total)
	return nil

}

// Restore fulfills an HSM Restore request
func (m *Mover) Restore(action dmplugin.Action) error {
	debug.Printf("%s id:%d restore %s %s", m.Name, action.ID(), action.PrimaryPath(), action.UUID())
	rate.Mark(1)

	start := time.Now()
	if action.UUID() == "" {
		return errors.Errorf("Missing file_id on action %d", action.ID())
	}

	srcObj := string(action.UUID())
	objAttrs, err := m.GcsClient.Bucket(m.Bucket).Object(srcObj).Attrs(m.Ctx)
	if err != nil {
		return err
	}

	debug.Printf("obj %s, size %d", srcObj, objAttrs.Size)

	dstSize := objAttrs.Size
	dst, err := dmio.NewActionWriter(action)
	if err != nil {
		return errors.Wrapf(err, "Couldn't create ActionWriter for %s", action)
	}
	defer dst.Close()

	progressFunc := func(offset, length int64) error {
		return action.Update(offset, length, dstSize)
	}
	progressWriter := dmio.NewProgressWriter(dst, updateInterval, progressFunc)
	defer progressWriter.StopUpdates()

	reader, err := m.newReader(srcObj)
	n, err := download(progressWriter, reader)
	if err != nil {
		return errors.Errorf("gcs.Download() of %s failed: %s", srcObj, err)
	}

	debug.Printf("%s id:%d Restored %d bytes in %v from %s to %s", m.Name, action.ID(), n,
		time.Since(start),
		srcObj,
		action.PrimaryPath())
	action.SetActualLength(n)
	return nil
}

// Remove fulfills an HSM Remove request
func (m *Mover) Remove(action dmplugin.Action) error {
	debug.Printf("%s id:%d remove %s %s", m.Name, action.ID(), action.PrimaryPath(), action.UUID())
	rate.Mark(1)
	if action.UUID() == "" {
		return errors.New("Missing file_id")
	}

	o := m.GcsClient.Bucket(m.Bucket).Object(string(action.UUID()))
	err := o.Delete(m.Ctx)
	if err != nil {
		return err
	}
	return errors.Wrap(err, "delete object failed")
}
