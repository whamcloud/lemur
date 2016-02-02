// Prototype HSM Copytool
package main

import (
	"log"
	"sync"

	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/hsm"
	"golang.org/x/net/context"

	"github.com/golang/glog"
)

type (
	// CopyTool for a single filesytem and a collection of backends.
	CopyTool struct {
		root     fs.RootDir
		backends map[uint]Backend
		agent    hsm.Agent
		wg       sync.WaitGroup
	}
)

func (ct *CopyTool) Stop() {
	if ct.agent != nil {
		ct.agent.Stop()
	}
}

func (ct *CopyTool) initAgent(ctx context.Context) error {
	var err error
	ct.agent, err = hsm.Start(ctx, ct.root)

	if err != nil {
		return err
	}

	return nil
}

func (ct *CopyTool) initBackends(conf *HSMConfig) error {
	ct.backends = make(map[uint]Backend, 0)
	root, err := fs.MountRoot(conf.Lustre)
	if err != nil {
		glog.Fatal(err)
	}
	for _, a := range conf.Archives {
		glog.V(3).Info(a)
		switch a.Type {
		case "mirror":
			{
				ct.backends[a.ArchiveID] = NewS3MirrorBackend(root)
			}
		case "posix":
			{
				ct.backends[a.ArchiveID] = NewPosixBackend(root, a.PosixDir, a.SnapshotsEnabled)
			}
		case "s3":
			{
				ct.backends[a.ArchiveID] = NewS3Backend(root, a.S3Url)
			}
		default:
			{
				ct.backends[a.ArchiveID] = NewNoopBackend(root)
			}
		}
		glog.Infof("created: %d %s", a.ArchiveID, ct.backends[a.ArchiveID])

	}
	return nil
}

func (ct *CopyTool) addHandler() {
	ct.wg.Add(1)
	go func() {
		defer ct.wg.Done()
		handleActions(ct)
	}()
}

// GetBackend returns the archive handler for the ArchiveID.
func (ct *CopyTool) GetBackend(i uint) (Backend, bool) {
	be, ok := ct.backends[i]
	return be, ok
}

// Actions returns the channel for receiving hsm.ActionRequests.
func (ct *CopyTool) Actions() <-chan hsm.ActionRequest {
	return ct.agent.Actions()
}

func copytool(conf *HSMConfig) {
	root, err := fs.MountRoot(conf.Lustre)
	if err != nil {
		glog.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ct := &CopyTool{root: root}

	interruptHandler(func() {
		cancel()
		ct.Stop()
	})

	// Start copytool backends in the background
	go func() {

		ct.initBackends(conf)
		err := ct.initAgent(ctx)
		if err != nil {
			log.Fatal(err)
		}

		for i := 0; i < conf.Processes; i++ {
			ct.addHandler()
		}
	}()

	<-ctx.Done()
	ct.wg.Wait()
}

func main() {
	defer glog.Flush()

	conf := configInitMust()
	glog.V(2).Infof("current configuration:\n%v", conf.String())

	copytool(conf)
}
