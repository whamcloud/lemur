package client

import (
	"syscall"

	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/pkg/mntent"
)

type (
	// FsID is a Lustre filesystem ID
	FsID struct {
		val [2]int32
	}

	// Client defines an interface for Lustre filesystem clients
	Client interface {
		FsName() string
		Path() string
		Root() fs.RootDir
	}

	// FsClient is an implementation of the Client interface
	fsClient struct {
		root   fs.RootDir
		fsName string
		fsID   *FsID
	}
)

func getFsName(mountPath string) (string, error) {
	entry, err := mntent.GetEntryByDir(mountPath)
	if err != nil {
		return "", err
	}
	return entry.Fsname, nil
}

func getFsID(mountPath string) (*FsID, error) {
	statfs := &syscall.Statfs_t{}

	if err := syscall.Statfs(mountPath, statfs); err != nil {
		return nil, err
	}
	var id FsID
	id.val[0] = statfs.Fsid.X__val[0]
	id.val[1] = statfs.Fsid.X__val[1]
	return &id, nil
}

// New returns a new Client
func New(path string) (*fsClient, error) {
	root, err := fs.MountRoot(path)
	if err != nil {
		return nil, err
	}
	name, err := getFsName(root.Path())
	if err != nil {
		return nil, err
	}
	id, err := getFsID(path)
	if err != nil {
		return nil, err
	}
	return &fsClient{root: root,
		fsName: name,
		fsID:   id,
	}, nil
}

// FsName returns the filesystem name
func (c *fsClient) FsName() string {
	return c.fsName
}

// Path returns the filesystem root path
func (c *fsClient) Path() string {
	return c.root.Path()
}

// Root returns the underlying fs.RootDir item
func (c *fsClient) Root() fs.RootDir {
	return c.root
}
