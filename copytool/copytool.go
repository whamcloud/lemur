// Prototype HSM Copytool
package main

import (
	"sync"

	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/audit"
	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/hsm"
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

func (ct *CopyTool) initAgent() (err error) {
	ct.agent, err = hsm.Start(ct.root)
	return
}

func (ct *CopyTool) initBackends(conf *HSMConfig) error {
	ct.backends = make(map[uint]Backend, 0)
	root, err := fs.MountRoot(conf.Lustre)
	if err != nil {
		alert.Fatal(err)
	}
	for _, a := range conf.Archives {
		audit.Log(a)
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
		audit.Logf("created: %d %s", a.ArchiveID, ct.backends[a.ArchiveID])

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
		alert.Fatal(err)
	}

	ct := &CopyTool{root: root}

	interruptHandler(func() {
		ct.Stop()
	})

	// Start copytool backends in the background
	go func() {

		ct.initBackends(conf)
		err := ct.initAgent()
		if err != nil {
			alert.Fatal(err)
		}

		for i := 0; i < conf.Processes; i++ {
			ct.addHandler()
		}
	}()

	ct.wg.Wait()
}

func main() {
	conf := configInitMust()
	audit.Logf("current configuration:\n%v", conf.String())

	copytool(conf)
}
