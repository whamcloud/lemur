package main

import (
	"flag"
	"os"
	"path"

	"github.com/pkg/errors"

	"github.intel.com/hpdd/lemur/dmplugin"
	"github.intel.com/hpdd/lemur/pkg/fsroot"
	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/debug"
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
