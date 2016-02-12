package main

import (
	"fmt"

	"code.google.com/p/go-uuid/uuid"
	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/policy/pdm/dmplugin"
	"github.intel.com/hpdd/policy/pkg/client"
)

// Mover is a POSIX data mover
type Mover struct {
	name      string
	client    *client.Client
	bucket    string
	prefix    string
	archiveID uint32
}

// S3Mover returns a new *Mover
func S3Mover(c *client.Client, archiveID uint32, bucket string, prefix string) *Mover {
	return &Mover{
		name:      fmt.Sprintf("posix-%d", archiveID),
		client:    c,
		archiveID: archiveID,
		bucket:    bucket,
		prefix:    prefix,
	}
}

// FsName returns the name of the associated Lustre filesystem
func (m *Mover) FsName() string {
	return m.client.FsName()
}

// ArchiveID returns the HSM archive number associated with this data mover
func (m *Mover) ArchiveID() uint32 {
	return m.archiveID
}

func newFileID() string {
	return uuid.New()
}

// Archive fulfills an HSM Archive request
func (m *Mover) Archive(action *dmplugin.Action) error {
	debug.Printf("%s id:%d archive %s %s", m.name, action.ID(), action.PrimaryPath(), action.FileID())
	return fmt.Errorf("id:%d  archive not implmented", action.ID())
}

// Restore fulfills an HSM Restore request
func (m *Mover) Restore(action *dmplugin.Action) error {
	debug.Printf("%s id:%d restore %s %s", m.name, action.ID(), action.PrimaryPath(), action.FileID())
	return fmt.Errorf("id:%d  restore not implmented", action.ID())
}

// Remove fulfills an HSM Remove request
func (m *Mover) Remove(action *dmplugin.Action) error {
	debug.Printf("%s id:%d remove %s %s", m.name, action.ID(), action.PrimaryPath(), action.FileID())
	return fmt.Errorf("id:%d  remove not implmented", action.ID())
}
