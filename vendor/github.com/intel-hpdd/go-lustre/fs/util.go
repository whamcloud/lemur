// Copyright (c) 2016 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package fs

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"syscall"

	"github.com/intel-hpdd/go-lustre/luser"
)

// Version returns the current Lustre version string.
func Version() (string, error) {
	v, err := luser.GetVersion()
	if err != nil {
		return "", fmt.Errorf("GetVersion() failed: %s", err)
	}
	return v.Lustre, nil
}

// RootDir represent a the mount point of a Lustre filesystem.
type RootDir struct {
	path string
}

// IsValid indicates whether or not the RootDir is actually the
// root of a Lustre filesystem.
func (root RootDir) IsValid() bool {
	return isDotLustre(path.Join(root.path, ".lustre"))
}

// Join args with root dir to create an absolute path.
// FIXME: replace this with OpenAt and friends
func (root RootDir) Join(args ...string) string {
	return path.Join(root.path, path.Join(args...))
}

func (root RootDir) String() string {
	return root.path
}

// Path returns the path for the root
func (root RootDir) Path() string {
	return root.path
}

// Open returns open handle for the root directory
func (root RootDir) Open() (*os.File, error) {
	return os.Open(root.path)
}

// ID should be a unique identifier for a filesystem. For now just use RootDir
type ID RootDir

func (id ID) String() string {
	return id.path
}

// Path returns the path for the root
func (id ID) Path() (string, error) {
	return id.path, nil
}

// Root returns the root dir for the root
func (id ID) Root() (RootDir, error) {
	return RootDir(id), nil
}

// GetID returns the filesystem's ID. For the moment, this is the root path, but in
// the future it could be something more globally unique (uuid?).
func GetID(p string) (ID, error) {
	r, err := MountRoot(p)
	if err != nil {
		return ID(r), err
	}
	return ID(r), nil
}

// Determine if given directory is the one true magical DOT_LUSTRE directory.
func isDotLustre(dir string) bool {
	fi, err := os.Lstat(dir)
	if err != nil {
		return false
	}
	if fi.IsDir() {
		fid, err := LookupFid(dir)
		if err == nil && fid.IsDotLustre() {
			return true
		}
	}
	return false
}

// Return root device from the struct stat embedded in FileInfo
func rootDevice(fi os.FileInfo) uint64 {
	stat, ok := fi.Sys().(*syscall.Stat_t)
	if ok {
		return uint64(stat.Dev)
	}
	panic("no stat available")
}

// findRoot returns the root directory for the lustre filesystem containing
// the pathname. If the the filesystem is not lustre, then error is returned.
func findRoot(dev uint64, pathname string) string {
	parent := path.Dir(pathname)
	fi, err := os.Lstat(parent)
	if err != nil {
		return ""
	}
	//  If "/" is lustre then we won't see the device change
	if rootDevice(fi) != dev || pathname == "/" {
		if isDotLustre(path.Join(pathname, ".lustre")) {
			return pathname
		}
		return ""
	}

	return findRoot(dev, parent)
}

// MountRoot returns the Lustre filesystem mountpoint for path
// or returns an error if the path is not on a Lustre filesystem.
func MountRoot(path string) (RootDir, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return RootDir{}, err
	}
	fi, err := os.Lstat(absPath)
	if err != nil {
		return RootDir{}, err
	}
	mnt := findRoot(rootDevice(fi), absPath)
	if mnt == "" {
		return RootDir{}, fmt.Errorf("%s not a Lustre filesystem", path)
	}
	return RootDir{path: mnt}, nil
}

// findRelPah returns pathname relative to root directory for the lustre filesystem containing
// the pathname. If no Lustre root was found, then empty strings are returned.
func findRelPath(dev uint64, pathname string, relPath []string) (string, string) {
	parent := path.Dir(pathname)
	fi, err := os.Lstat(parent)
	if err != nil {
		return "", ""
	}
	//  If "/" is lustre then we won't see the device change
	if rootDevice(fi) != dev || pathname == "/" {
		if isDotLustre(path.Join(pathname, ".lustre")) {
			return pathname, path.Join(relPath...)
		}
		return "", ""
	}

	return findRelPath(dev, parent, append([]string{path.Base(pathname)}, relPath...))
}

// MountRelPath returns the lustre mountpoint, and remaing path for
// the given pathname. The remaining path is relative to the mount
// point. Returns an error if pathname is not valid or does not refer
// to a Lustre fs.
func MountRelPath(pathname string) (RootDir, string, error) {
	pathname = filepath.Clean(pathname)
	fi, err := os.Lstat(pathname)
	if err != nil {
		return RootDir{}, "", err
	}

	root, relPath := findRelPath(rootDevice(fi), pathname, []string{})
	if root == "" {
		return RootDir{}, "", fmt.Errorf("%s not a Lustre filesystem", pathname)
	}
	return RootDir{path: root}, relPath, nil
}
