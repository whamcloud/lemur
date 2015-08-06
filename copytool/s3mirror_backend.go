package main

import (
	"fmt"
	"io"
	"net/url"
	"os"

	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/hsm"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang/glog"
)

type (
	S3MirrorBackend struct {
		root fs.RootDir
		s    *s3.S3
	}
)

func NewS3MirrorBackend(root fs.RootDir) *S3MirrorBackend {
	// Open S3 connection
	s := s3.New(&aws.Config{Region: aws.String("us-east-1")})

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

	result, err := be.s.GetObject(&s3.GetObjectInput{
		Bucket: &u.Host,
		Key:    &u.Path,
	})
	if err != nil {
		glog.Error(err)
		return ErrorResult(err, -1)
	}
	defer result.Body.Close()

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

	n, err := io.Copy(out, result.Body)
	if err != nil {
		glog.Error(err)
		return ErrorResult(err, -1)
	}
	return GoodResult(0, uint64(n))
}
