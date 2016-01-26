package client

import (
	"syscall"

	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/pkg/mntent"
)

type FsID struct {
	val [2]int32
}
type Client struct {
	root   fs.RootDir
	fsName string
	fsID   *FsID
}

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

func New(path string) (*Client, error) {
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
	return &Client{root: root,
		fsName: name,
		fsID:   id,
	}, nil
}

func (c *Client) FsName() string {
	return c.fsName
}

func (c *Client) Path() string {
	return c.root.Path()
}

func (c *Client) Root() fs.RootDir {
	return c.root
}
