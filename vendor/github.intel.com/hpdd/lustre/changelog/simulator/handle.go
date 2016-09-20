package simulator

import (
	"io"

	"github.intel.com/hpdd/lustre/changelog"
)

type simHandle struct {
	sim    *Simulator
	follow bool
}

func (h *simHandle) Open(follow bool) error {
	return h.OpenAt(1, follow)
}

func (h *simHandle) OpenAt(startRec int64, follow bool) error {
	h.follow = follow
	return nil
}

func (h *simHandle) Close() error {
	return nil
}

func (h *simHandle) NextRecord() (changelog.Record, error) {
	for {
		rec, err := h.sim.NextRecord()
		if err == io.EOF && h.follow {
			continue
		}
		return rec, err
	}
}

func (h *simHandle) Clear(token string, endRec int64) error {
	return nil
}

func (h *simHandle) String() string {
	return ""
}
