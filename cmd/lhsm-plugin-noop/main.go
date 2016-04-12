package main

import (
	"flag"

	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/policy/pdm/dmplugin"
)

var (
	archive      uint
	agentAddress string
)

func init() {
	flag.UintVar(&archive, "archive", 1, "archive id")
	flag.StringVar(&agentAddress, "agent", ":4242", "Lustre client mountpoint")
}

// Mover is a NOOP data mover
type Mover struct {
}

func noop(agentAddress string) {
	done := make(chan struct{})

	plugin, err := dmplugin.New(agentAddress)
	if err != nil {
		alert.Fatal(err)
	}

	mover := Mover{}
	plugin.AddMover(&dmplugin.Config{
		Mover:     &mover,
		FsName:    "noop",
		ArchiveID: uint32(archive),
	})

	<-done
	plugin.Stop()
}

func main() {
	flag.Parse()

	noop(agentAddress)
}
