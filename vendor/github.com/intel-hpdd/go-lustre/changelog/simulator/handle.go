// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package simulator

import (
	"io"

	"github.com/intel-hpdd/go-lustre/changelog"
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
