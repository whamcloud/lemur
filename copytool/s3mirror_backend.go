package main

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"time"

	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/hsm"

	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"github.com/golang/glog"
)

type (
	S3MirrorBackend struct {
		root fs.RootDir
		s    *s3.S3
	}
)

func NewS3MirrorBackend(root fs.RootDir) *S3MirrorBackend {
	auth, err := aws.GetAuth("", "", "", time.Time{})
	if err != nil {
		glog.Fatal(err)
	}

	// Open S3 connection
	s := s3.New(auth, aws.USEast)

	return &S3MirrorBackend{
		root: root,
		s:    s}
}

func (back S3MirrorBackend) String() string {
	return fmt.Sprintf("S3Mirror backend for %v", back.root)
}

func (be S3MirrorBackend) Restore(aih hsm.ActionHandle) ActionResult {
	names, err := fs.FidPathnames(be.root, aih.Fid())
	if err != nil {
		// ?
	}
	glog.Infof("%v %v\n", aih, names)

	rawurl, err := fileUrl(be.root, aih.Fid())
	if err != nil {
		glog.Error(err)
		return ErrorResult(err, -1)
	}
	u, err := url.Parse(rawurl)
	if err != nil {
		glog.Error(err)
		return ErrorResult(err, -1)
	}
	bucket := be.s.Bucket(u.Host)
	in, err := bucket.GetReader(u.Path)
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
	out, err := fs.OpenFileByFid(be.root, dataFid, os.O_WRONLY, 0x666)
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
