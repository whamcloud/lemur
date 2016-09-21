// This utility doesn't really "do" anything... It's here as an example
// of how to set up and run a simulator. Can probably be nuked if/when
// the simulator is integrated into some other code.
package main

import (
	"fmt"
	"io"

	"github.intel.com/hpdd/lustre/changelog/simulator"
)

func main() {
	//go metrics.Log(metrics.DefaultRegistry, 10e9, log.New(os.Stderr, "metrics: ", log.Lmicroseconds))

	sim, err := simulator.New()
	if err != nil {
		panic(err)
	}
	if err = sim.AddJob(
		simulator.OptJobID("test"),
		simulator.OptJobMaxFileCount(1024),
	); err != nil {
		panic(err)
	}
	if err = sim.AddJob(
		simulator.OptJobID("test1"),
		simulator.OptJobMaxFileCount(16384),
	); err != nil {
		panic(err)
	}
	if err = sim.AddJob(
		simulator.OptJobID("test2"),
		simulator.OptJobMaxFileCount(5242880),
	); err != nil {
		panic(err)
	}
	sim.Start()

	h := sim.GetHandle()
	h.Open(false)
	defer h.Close()

	var seenRecords int64
	var missing []int64
	rec, err := h.NextRecord()
	for ; err == nil; rec, err = h.NextRecord() {
		seenRecords++
		if rec.Index() != seenRecords {
			missing = append(missing, seenRecords)
		}
	}
	if err != nil && err != io.EOF {
		panic(err)
	}

	sim.Stop()
	fmt.Printf("Received %d records (%d missing).\n", seenRecords, len(missing))
	fmt.Println(sim.Stats())
}
