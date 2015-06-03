// +build ignore

package main

import (
	"fmt"
	"io"
	"os"
	"path"

	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/hsm"
	"github.intel.com/hpdd/policy/pdm"

	"github.com/golang/glog"
)

type (
	PosixBackend struct {
		root       fs.RootDir
		archiveDir string
		snapshots  bool
	}
)

func NewPosixBackend(root fs.RootDir, posixDir string, snapshots bool) *PosixBackend {
	return &PosixBackend{
		root:       root,
		archiveDir: posixDir,
		snapshots:  snapshots,
	}
}

func (back PosixBackend) String() string {
	return fmt.Sprintf("Posix backend for %v, %v\n", back.root, back.archiveDir)
}

func (back PosixBackend) destination(key string) string {
	dir := path.Join(
		back.archiveDir,
		"objects",
		fmt.Sprintf("%s", key[0:2]),
		fmt.Sprintf("%s", key[2:4]))

	err := os.MkdirAll(dir, 0600)
	if err != nil {
		panic(err)
	}
	return path.Join(dir, key)
}

func CopyWithProgress(dst io.WriterAt, src io.ReaderAt, max uint64, aih hsm.ActionHandle) (uint64, error) {
	offset := aih.Offset()
	blockSize := 10 * 1024 * 1024 // FIXME: parameterize
	for offset < max {

		err := aih.Progress(aih.Offset(), offset, max, 0)
		if err != nil {
			return offset, err
		}

		n, err := CopyAt(dst, src, int64(offset), blockSize)
		offset += uint64(n)

		if n < blockSize && err == io.EOF {
			break
		}

		if err != nil {
			return offset, err
		}
	}
	return offset, nil
}

// Archive copies the file contents to an object in a local directory
func (back PosixBackend) Archive(aih *pdm.Request) *pdm.Result {
	fileKey, err = getFileId(back.root, aih.SourcePath)
	if err != nil {
		return ErrorResult(err, -1)
	}

	glog.Infof("%v %v %v", aih, aih.SourcePath, fileKey)

	dataFid, err := aih.DataFid()
	if err != nil {
		return ErrorResult(err, -1)
	}

	fi, err := fs.StatFid(back.root, dataFid)
	if err != nil {
		return ErrorResult(err, -1)
	}

	in, err := fs.OpenByFid(back.root, dataFid)
	if err != nil {
		return ErrorResult(err, -1)
	}
	defer in.Close()

	out, err := os.Create(back.destination(fileKey))
	if err != nil {
		return ErrorResult(err, -1)
	}
	defer out.Close()

	n, err := CopyWithProgress(out, in, uint64(fi.Size()), aih)
	if err != nil {
		return ErrorResult(err, -1)
	}

	if back.snapshots {
		err := createSnapshots(back.root, aih.ArchiveID(), fileKey, names)
		if err != nil {
			glog.Infoln("snapshot failed: ", err)
		}
	}

	return GoodResult(0, n)
}

// Restore copies file data from a local object to original Lustre file
func (back PosixBackend) Restore(aih *pdm.Request) *pdm.Result {
	names, err := fs.FidPathnames(back.root, aih.Fid())
	if err != nil {
		return ErrorResult(err, -1)
	}
	fileKey, err := fileID(back.root, aih.Fid())
	if err != nil {
		return ErrorResult(err, -1)
	}

	glog.Infof("%v %v %v", aih, names, fileKey)

	dataFid, err := aih.DataFid()
	if err != nil {
		return ErrorResult(err, -1)
	}

	out, err := fs.OpenFileByFid(back.root, dataFid, os.O_WRONLY, 0644)
	if err != nil {
		return ErrorResult(err, -1)
	}
	defer out.Close()

	fi, err := os.Stat(back.destination(fileKey))
	if err != nil {
		return ErrorResult(err, -1)
	}

	in, err := os.Open(back.destination(fileKey))
	if err != nil {
		return ErrorResult(err, -1)
	}
	defer in.Close()

	n, err := CopyWithProgress(out, in, uint64(fi.Size()), aih)
	if err != nil {
		return ErrorResult(err, -1)
	}
	return GoodResult(0, uint64(n))
}

// Remove deletes the object in the backing store
func (back PosixBackend) Remove(aih *pdm.Request) *pdm.Result {
	names, err := fs.FidPathnames(back.root, aih.Fid())
	if err != nil {
		return ErrorResult(err, -1)
	}

	fileKey, err := fileID(back.root, aih.Fid())
	if err != nil {
		return ErrorResult(err, -1)
	}

	glog.Infof("%v %v %v", aih, names, fileKey)

	dest := back.destination(fileKey)

	// TODO: Also remove empty directories above the file?
	err = os.Remove(dest)
	if err != nil {
		return ErrorResult(err, -1)
	}
	return GoodResult(0, 0)
}
