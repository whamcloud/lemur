package main

import (
	"flag"
	"log"

	"github.intel.com/hpdd/policy/pdm/dmplugin"
	pb "github.intel.com/hpdd/policy/pdm/pdm"
	"google.golang.org/grpc"
)

var (
	archive uint
)

func init() {
	flag.UintVar(&archive, "archive", 1, "archive id")
}

type Mover struct {
	fsName    string
	archiveID uint32
}

func (m *Mover) FsName() string {
	return m.fsName
}

func (m *Mover) ArchiveID() uint32 {
	return m.archiveID
}

func noop(client pb.DataMoverClient) {
	done := make(chan struct{})
	mover := Mover{fsName: "noop", archiveID: uint32(archive)}
	dm := dmplugin.New(client, &mover)
	dm.Run()
	<-done
}

func main() {
	flag.Parse()

	conn, err := grpc.Dial("localhost:4242", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()
	client := pb.NewDataMoverClient(conn)

	noop(client)
}
