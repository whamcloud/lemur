// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package main

import (
	"os"
	"os/user"
	"strconv"
	"syscall"
	"time"

	"github.com/intel-hpdd/go-lustre/hsm"
	"github.com/intel-hpdd/go-lustre/llapi"
	"github.com/intel-hpdd/lemur/cmd/lhsmd/agent/fileid"
	"github.com/intel-hpdd/logging/debug"
	"github.com/pkg/errors"
	cli "gopkg.in/urfave/cli.v1"
)

func strtou32(s string) (uint32, error) {
	v, err := strconv.ParseUint(s, 0, 32)
	if err != nil {
		return 0, err
	}
	return uint32(v), nil
}

func lookupUser(s string) (uint32, error) {
	if s == "" {
		return 0, errors.New("no user or uid specified")
	}

	if val, err := strtou32(s); err == nil {
		return val, nil
	}

	u, err := user.Lookup(s)
	if err != nil {
		return 0, err
	}

	return strtou32(u.Uid)
}

func lookupGroup(s string) (uint32, error) {
	if s == "" {
		return 0, errors.New("no group or groupd id specified")
	}

	if val, err := strtou32(s); err == nil {
		return val, nil
	}

	g, err := user.LookupGroup(s)
	if err != nil {
		return 0, err
	}

	return strtou32(g.Gid)

}

func getTimeOpt(c *cli.Context, name string, dflt time.Time) (time.Time, error) {
	value := dflt
	if c.String(name) != "" {
		t, err := parseTimestamp(c.String("timefmt"), c.String(name))
		if err != nil {
			return value, errors.Wrap(err, "Unable to parse mtime")
		}
		value = t
	}
	return value, nil
}

func parseTimestamp(timefmt, s string) (time.Time, error) {
	t, err := time.Parse(timefmt, s)
	if err != nil {
		return t, err
	}
	return t, err
}

// Construct a FileInfo compatible struct.
type myFileInfo struct {
	name string         // base name of the file
	stat syscall.Stat_t // underlying data source (can return nil)
}

func (fi *myFileInfo) Name() string {
	return fi.name
}

func (fi *myFileInfo) Size() int64 {
	return fi.stat.Size
}

func (fi *myFileInfo) Mode() os.FileMode {
	return os.FileMode(fi.stat.Mode)
}

func (fi *myFileInfo) ModTime() time.Time {
	return time.Unix(fi.stat.Mtim.Sec, fi.stat.Mtim.Nsec)
}

func (fi *myFileInfo) IsDir() bool {
	return fi.Mode().IsDir()
}
func (fi *myFileInfo) Sys() interface{} {
	return &fi.stat
}

func hsmImportAction(c *cli.Context) error {
	logContext(c)
	archive := c.Uint("id")
	uuid := c.String("uuid")
	hash := c.String("hash")
	args := c.Args()
	if len(args) != 1 {
		return errors.New("HSM import only supports one file")
	}

	uid, err := lookupUser(c.String("uid"))
	if err != nil {
		return errors.Wrap(err, "Valid user required.")

	}

	gid, err := lookupGroup(c.String("gid"))
	if err != nil {
		return errors.Wrap(err, "Valid group required.")
	}

	mtime, err := getTimeOpt(c, "mtime", time.Now())
	if err != nil {
		return errors.Wrap(err, "Unable to parse mtime")
	}

	atime, err := getTimeOpt(c, "atime", mtime)
	if err != nil {
		return errors.Wrap(err, "Unable to parse atime")
	}

	fi := &myFileInfo{}
	fi.name = args[0]

	stat := &fi.stat
	stat.Uid = uid
	stat.Gid = gid
	stat.Mode = uint32(c.Uint("mode"))
	stat.Size = c.Int64("size")
	stat.Atim.Sec = int64(atime.Unix())
	stat.Atim.Nsec = int64(atime.Nanosecond())
	stat.Mtim.Sec = int64(mtime.Unix())
	stat.Mtim.Nsec = int64(atime.Nanosecond())

	layout := llapi.DefaultDataLayout()
	layout.StripeCount = c.Int("stripe_count")
	layout.StripeSize = c.Int("stripe_size")

	debug.Printf("%v, %v, %v, %v", archive, uuid, hash, args[0])
	_, err = hsm.Import(args[0], archive, fi, layout)
	if err != nil {
		return errors.Wrap(err, "Import failed")
	}

	if uuid != "" {
		fileid.UUID.Set(args[0], []byte(uuid))
	}

	if hash != "" {
		fileid.Hash.Set(args[0], []byte(hash))
	}

	return nil
}
