package main

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"time"

	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/hsm"

	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"github.com/golang/glog"
)

type (
	// S3Backend provides an HSM interface for S3.
	S3Backend struct {
		root       fs.RootDir
		s          *s3.S3
		bucketName string
		bucket     *s3.Bucket
		prefix     string
	}
)

// NewS3Backend initializes an S3 backend object.
func NewS3Backend(root fs.RootDir, rawurl string) *S3Backend {
	auth, err := aws.GetAuth("", "", "", time.Time{})
	if err != nil {
		glog.Fatal(err)
	}

	u, err := url.Parse(rawurl)
	if err != nil {
		glog.Error(err)
		panic(err)
	}

	// Open Bucket
	s := s3.New(auth, aws.USEast)
	bucket := s.Bucket(u.Host)

	return &S3Backend{
		root:       root,
		bucketName: u.Host,
		prefix:     u.Path,
		s:          s,
		bucket:     bucket,
	}
}

func (back S3Backend) String() string {
	return fmt.Sprintf("S3 backend for %v  s3://%s/%s", back.root, back.bucketName, back.prefix)
}

func (back S3Backend) destination(id string) string {
	key := path.Join(
		back.prefix,
		"objects",
		fmt.Sprintf("%s", id[0:2]),
		fmt.Sprintf("%s", id[2:4]),
		id)
	return key

}

// Archive copies the file contents to an object in an S3 bucket.
func (back S3Backend) Archive(aih hsm.ActionHandle) ActionResult {
	names, err := fs.FidPathnames(back.root, aih.Fid())
	if err != nil {
		return ErrorResult(err, -1)
	}
	fileKey, err := newFileId(back.root, aih.Fid())
	if err != nil {
		return ErrorResult(err, -1)
	}

	glog.Infof("%v %v %v", aih, names, fileKey)

	dataFid, err := aih.DataFid()
	if err != nil {
		return ErrorResult(err, -1)
	}

	fi, err := fs.StatFid(back.root, dataFid)
	if err != nil {
		return ErrorResult(err, -1)
	}

	inFile, err := fs.OpenByFid(back.root, dataFid)
	if err != nil {
		return ErrorResult(err, -1)
	}
	defer inFile.Close()

	keyName := back.destination(fileKey)
	err = back.bucket.PutReader(keyName, inFile, fi.Size(), "application/octet-stream", s3.Private, s3.Options{})
	if err != nil {
		glog.Error(err)
		return ErrorResult(err, -1)
	}

	return GoodResult(0, uint64(fi.Size()))
}

// Restore retrieves data from backend
func (back S3Backend) Restore(aih hsm.ActionHandle) ActionResult {
	names, err := fs.FidPathnames(back.root, aih.Fid())
	if err != nil {
		return ErrorResult(err, -1)
	}

	fileID, err := fileID(back.root, aih.Fid())
	if err != nil {
		glog.Error(err)
		return ErrorResult(err, -1)
	}

	glog.Infof("%v %v %v\n", aih, names, fileID)

	keyName := back.destination(fileID)
	in, err := back.bucket.GetReader(keyName)
	if err != nil {
		glog.Error(err)
		return ErrorResult(err, -1)
	}
	defer in.Close()

	dataFid, err := aih.DataFid()
	if err != nil {
		glog.Error(err)
		return ErrorResult(err, -1)
	}
	out, err := fs.OpenFileByFid(back.root, dataFid, os.O_WRONLY, 0x666)
	if err != nil {
		glog.Error(err)
		return ErrorResult(err, -1)
	}
	defer out.Close()

	n, err := io.Copy(out, in)
	if err != nil {
		glog.Error(err)
		return ErrorResult(err, -1)
	}

	return GoodResult(0, uint64(n))
}
