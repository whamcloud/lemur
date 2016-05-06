package main

import (
	"fmt"
	"os"
	"path"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/pborman/uuid"
	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/policy/pdm/dmplugin"
	"github.intel.com/hpdd/policy/pkg/client"
)

// Mover is an S3 data mover
type Mover struct {
	name   string
	client client.Client
	s3Svc  *s3.S3
	bucket string
	prefix string
}

// S3Mover returns a new *Mover
func S3Mover(c client.Client, s3Svc *s3.S3, archiveID uint32, bucket string, prefix string) *Mover {
	return &Mover{
		name:   fmt.Sprintf("s3-%d", archiveID),
		client: c,
		s3Svc:  s3Svc,
		bucket: bucket,
		prefix: prefix,
	}
}

// Base returns the base path in which the mover is operating
func (m *Mover) Base() string {
	return m.client.Path()
}

func newFileID() string {
	return uuid.New()
}

func (m *Mover) destination(id string) string {
	return path.Join(m.prefix,
		"objects",
		fmt.Sprintf("%s", id[0:2]),
		fmt.Sprintf("%s", id[2:4]),
		id)
}

func (m *Mover) newUploader() *s3manager.Uploader {
	// can configure stuff here with custom setters, e.g.
	// var partSize10 = func(u *Uploader) {
	//     u.PartSize = 1024 * 1024 * 10
	// }
	// s3manager.NewUploaderWithClient(m.s3Svc, partSize10)
	return s3manager.NewUploaderWithClient(m.s3Svc)
}

func (m *Mover) newDownloader() *s3manager.Downloader {
	return s3manager.NewDownloaderWithClient(m.s3Svc)
}

// Archive fulfills an HSM Archive request
func (m *Mover) Archive(action dmplugin.Action) error {
	debug.Printf("%s id:%d archive %s %s", m.name, action.ID(), action.PrimaryPath(), action.FileID())
	rate.Mark(1)
	start := time.Now()

	src, err := os.Open(path.Join(m.Base(), action.PrimaryPath()))
	if err != nil {
		return err
	}
	defer src.Close()

	fi, err := src.Stat()
	if err != nil {
		return err
	}

	fileID := newFileID()
	size := fi.Size()
	progressFunc := func(offset, length int64) error {
		return action.Update(offset, length, size)
	}
	progressReader := NewProgressReader(src, updateInterval, progressFunc)
	defer progressReader.StopUpdates()

	uploader := m.newUploader()
	out, err := uploader.Upload(&s3manager.UploadInput{
		Body:        progressReader,
		Bucket:      aws.String(m.bucket),
		Key:         aws.String(m.destination(fileID)),
		ContentType: aws.String("application/octet-stream"),
	})
	if err != nil {
		if multierr, ok := err.(s3manager.MultiUploadFailure); ok {
			alert.Warn("Upload error:", multierr.Code(), multierr.Message(), multierr.UploadID())
		}
		return err
	}

	debug.Printf("%s id:%d Archived %d bytes in %v from %s to %s", m.name, action.ID(), fi.Size(),
		time.Since(start),
		action.PrimaryPath(),
		out.Location)
	action.SetFileID([]byte(fileID))
	action.SetActualLength(uint64(fi.Size()))
	return nil
}

// Restore fulfills an HSM Restore request
func (m *Mover) Restore(action dmplugin.Action) error {
	debug.Printf("%s id:%d restore %s %s", m.name, action.ID(), action.PrimaryPath(), action.FileID())
	rate.Mark(1)

	start := time.Now()
	if action.FileID() == "" {
		return fmt.Errorf("Missing file_id on action %d", action.ID())
	}

	srcObj := m.destination(action.FileID())
	out, err := m.s3Svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(srcObj),
	})
	if err != nil {
		return fmt.Errorf("s3.HeadObject() on %s failed: %s", srcObj, err)
	}
	debug.Printf("obj %s, size %d", srcObj, *out.ContentLength)

	dstSize := *out.ContentLength
	dstPath := path.Join(m.Base(), action.WritePath())
	dst, err := os.OpenFile(dstPath, os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("Couldn't open %s for write: %s", dstPath, err)
	}
	defer dst.Close()

	progressFunc := func(offset, length int64) error {
		return action.Update(offset, length, dstSize)
	}
	progressWriter := NewProgressWriter(dst, updateInterval, progressFunc)
	defer progressWriter.StopUpdates()

	downloader := m.newDownloader()
	n, err := downloader.Download(progressWriter,
		&s3.GetObjectInput{
			Bucket: aws.String(m.bucket),
			Key:    aws.String(srcObj),
		})
	if err != nil {
		return fmt.Errorf("s3.Download() of %s failed: %s", srcObj, err)
	}

	debug.Printf("%s id:%d Restored %d bytes in %v from %s to %s", m.name, action.ID(), n,
		time.Since(start),
		srcObj,
		action.PrimaryPath())
	action.SetActualLength(uint64(n))
	return nil
}

// Remove fulfills an HSM Remove request
func (m *Mover) Remove(action dmplugin.Action) error {
	debug.Printf("%s id:%d remove %s %s", m.name, action.ID(), action.PrimaryPath(), action.FileID())
	rate.Mark(1)

	_, err := m.s3Svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(m.destination(action.FileID())),
	})
	return err
}
