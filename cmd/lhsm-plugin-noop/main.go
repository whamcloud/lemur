package main

import (
	"flag"
	"os"
	"path"

	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/policy/pdm/dmplugin"
	"github.intel.com/hpdd/policy/pdm/lhsmd/config"
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

func noop(agentAddress string) {
	done := make(chan struct{})

	plugin, err := dmplugin.New(path.Base(os.Args[0]))
	if err != nil {
		alert.Fatal(err)
	}

	mover := Mover{}
	plugin.AddMover(&dmplugin.Config{
		Mover:     &mover,
		ArchiveID: uint32(archive),
	})

	<-done
	plugin.Close()
}

func main() {
	agentAddress := os.Getenv(config.AgentConnEnvVar)
	if agentAddress == "" {
		alert.Fatal("This plugin is intended to be launched by the agent.")
	}

	noop(agentAddress)
}
