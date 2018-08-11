// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"os"
	"path"

	"github.com/pkg/errors"

	"github.com/intel-hpdd/lemur/dmplugin"
	"github.com/intel-hpdd/lemur/pkg/fsroot"
	"github.com/intel-hpdd/logging/alert"
	"github.com/intel-hpdd/logging/debug"
)

var (
	archive uint
)

func init() {
	flag.UintVar(&archive, "archive", 1, "archive id")
}

// Mover is a NOOP data mover
type Mover struct {
}

// Start signals the mover to begin any asynchronous processing (e.g. stats)
func (m *Mover) Start() {
	debug.Print("noop mover started")
}

func noop() {
	done := make(chan struct{})

	plugin, err := dmplugin.New(path.Base(os.Args[0]), func(path string) (fsroot.Client, error) {
		return fsroot.New(path)
	})
	if err != nil {
		alert.Abort(errors.Wrap(err, "create plugin failed"))
	}

	mover := Mover{}
	plugin.AddMover(&dmplugin.Config{
		Mover:     &mover,
		ArchiveID: uint32(archive),
	})

	<-done
	if err = plugin.Close(); err != nil {
		alert.Abort(errors.Wrap(err, "close failed"))
	}
}

func main() {
	noop()
}
