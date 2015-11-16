/**

Parallel Data Mover is scalable system to copy or migrate data between
various storage systems. It supports multliple types of sources and
destinations, including POSIX, S3, HPSS, etc.

Use cases include:
  * Data movement for Lustre HSM.
  * Offsite replication for DR
  * Lustre file-level replication
  * Storage rebalancing within a single tier
  * Migration between filesytems (e.g GPFS - > Lustre)

Initially the main focus is for HSM.
*/

package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/golang/glog"
	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/policy/pdm"
	"github.intel.com/hpdd/policy/pkg/workq"
)

type (
	// Mover for a single filesytem and a collection of backends.
	Mover struct {
		root     fs.RootDir
		queue    *workq.Worker
		backends map[uint]Backend
		wg       sync.WaitGroup
	}
)

func (mover *Mover) initBackends(conf *pdm.HSMConfig) error {
	mover.backends = make(map[uint]Backend, 0)
	root, err := fs.MountRoot(conf.Lustre)
	if err != nil {
		glog.Fatal(err)
	}
	for _, a := range conf.Archives {
		glog.V(3).Info(a)
		switch a.Type {
		case "posix":
			//			{
			//				mover.backends[a.ArchiveID] = NewPosixBackend(root, a.PosixDir, a.SnapshotsEnabled)
			//			}
		// case "mirror":
		// 	{
		// 		mover.backends[a.ArchiveID] = NewS3MirrorBackend(root)
		// 	}
		// case "s3":
		// 	{
		// 		mover.backends[a.ArchiveID] = NewS3Backend(root, a.S3Url)
		// 	}
		default:
			{
				mover.backends[a.ArchiveID] = NewNoopBackend(root)
			}
		}
		glog.Infof("created: %d %s", a.ArchiveID, mover.backends[a.ArchiveID])

	}
	return nil
}

func (mover *Mover) Process(d workq.Delivery) error {
	mover.wg.Add(1)
	defer mover.wg.Done()
	var r pdm.Request
	if err := d.Payload(r); err != nil {
		return err
	}
	var backend Backend
	result, err := handleAction(backend, &r)
	log.Printf("received: %v %v\n", r, err)

	d.Status(result)
	return nil
}

func mover(conf *pdm.HSMConfig) {
	root, err := fs.MountRoot(conf.Lustre)
	if err != nil {
		glog.Fatal(err)
	}

	done := make(chan struct{})
	mover := &Mover{
		root:  root,
		queue: workq.NewWorker("pdm", conf.RedisServer),
	}

	interruptHandler(func() {
		close(done)
		os.Exit(0)
		//		mover.Stop()
	})

	go func() {
		if err != nil {
			log.Fatal(err)
			return
		}

		for i := 0; i < conf.Processes; i++ {
			mover.queue.AddProcessor(mover)
		}
	}()

	<-done
	mover.wg.Wait()
}

var (
	reset = false
	trace = false
)

func init() {
	flag.BoolVar(&reset, "reset", false, "Reset queue")
	flag.BoolVar(&trace, "trace", false, "Print redis trace")
}

func main() {
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	defer glog.Flush()
	conf := pdm.ConfigInitMust()

	/*
		if reset {
			workq.Reset("pdm", conf.RedisServer)
		}
	*/

	mover(conf)
}

func interruptHandler(once func()) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGQUIT, syscall.SIGTERM)

	go func() {
		stopping := false
		for sig := range c {
			glog.Infoln("signal received:", sig)
			if !stopping {
				stopping = true
				once()
			}
		}
	}()

}
