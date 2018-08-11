// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package status

import (
	"os"
	"sync"

	"github.com/intel-hpdd/go-lustre"
	"github.com/intel-hpdd/go-lustre/fs"
	"github.com/intel-hpdd/go-lustre/llapi"
	"github.com/intel-hpdd/go-lustre/pkg/pool"
)

type mountDir struct {
	path fs.RootDir
	f    *os.File
}

// A cache of file handles per lustre mount point. Currently used to fetch the host Mdt for a file,
// and in future used with openat to open relative paths.
// Could merge with RootDir and ensure RootDir is a singleton per client.
var mountPools map[fs.RootDir]*pool.Pool
var mapLock sync.Mutex

func init() {
	mountPools = make(map[fs.RootDir]*pool.Pool)
}

func (m *mountDir) String() string {
	return m.path.String()
}

func (m *mountDir) Close() error {
	return m.f.Close()
}

func (m *mountDir) GetMdt(in *lustre.Fid) (int, error) {
	mdtIndex, err := llapi.GetMdtIndexByFid(int(m.f.Fd()), in)
	if err != nil {
		return 0, err
	}
	return mdtIndex, nil
}

func openMount(root fs.RootDir) (mnt *mountDir, err error) {
	m := &mountDir{path: root}
	m.f, err = root.Open()
	if err != nil {
		return
	}
	mnt = m
	return
}

func getOpenMount(root fs.RootDir) (mnt *mountDir, err error) {
	mapLock.Lock()
	defer mapLock.Unlock()
	p, ok := mountPools[root]

	if !ok {
		alloc := func() (interface{}, error) {
			return openMount(root)
		}

		p, err = pool.New(root.String(), 1, 10, alloc)
		if err != nil {
			return
		}
		mountPools[root] = p
	}

	o, err := p.Get()
	if err != nil {
		return
	}
	mnt = o.(*mountDir)
	return
}

func putMount(mnt *mountDir) {
	mapLock.Lock()
	defer mapLock.Unlock()
	p := mountPools[mnt.path]
	p.Put(mnt)
}

// GetMdt returns the MDT index for a given Fid
func GetMdt(root fs.RootDir, f *lustre.Fid) (int, error) {
	// mnt, err := getOpenMount(root)
	// if err != nil {
	// 	return 0, err
	// }
	// defer putMount(mnt)
	// return mnt.GetMdt(f)
	fp, err := os.Open(root.Path())
	if err != nil {
		return 0, err
	}
	defer fp.Close()
	return llapi.GetMdtIndexByFid(int(fp.Fd()), f)
}
