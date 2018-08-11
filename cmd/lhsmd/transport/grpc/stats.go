// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package rpc

import (
	"bytes"
	"fmt"
	"runtime"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/rcrowley/go-metrics"
)

type messageStats struct {
	StartTime   time.Time
	StartSysMem uint64
	MaxSysMem   uint64
	Count       metrics.Counter
	Rate        metrics.Meter
	Latencies   metrics.Histogram
}

func (s *messageStats) String() string {
	var buf bytes.Buffer

	ms := runtime.MemStats{}
	runtime.ReadMemStats(&ms)
	if ms.Sys > s.MaxSysMem {
		s.MaxSysMem = ms.Sys
	}

	fmt.Fprintf(&buf, "mem usage (start/cur/max): %s/%s/%s\n",
		humanize.Bytes(s.StartSysMem),
		humanize.Bytes(ms.Sys),
		humanize.Bytes(s.MaxSysMem),
	)
	fmt.Fprintf(&buf, "runtime: %s\n", time.Since(s.StartTime))
	fmt.Fprintf(&buf, "  count: %s\n", humanize.Comma(s.Count.Count()))
	fmt.Fprintf(&buf, "msg/sec (1 min/5 min/15 min/inst): %s/%s/%s/%s\n",
		humanize.Comma(int64(s.Rate.Rate1())),
		humanize.Comma(int64(s.Rate.Rate5())),
		humanize.Comma(int64(s.Rate.Rate15())),
		humanize.Comma(int64(s.Rate.RateMean())),
	)
	fmt.Fprintln(&buf, "latencies:")
	fmt.Fprintf(&buf, "  min: %s\n", time.Duration(s.Latencies.Min()))
	fmt.Fprintf(&buf, " mean: %s\n", time.Duration(int64(s.Latencies.Mean())))
	fmt.Fprintf(&buf, "  max: %s\n", time.Duration(s.Latencies.Max()))

	return buf.String()
}

func newMessageStats() *messageStats {
	ms := runtime.MemStats{}
	runtime.ReadMemStats(&ms)

	return &messageStats{
		StartTime:   time.Now(),
		StartSysMem: ms.Sys,
		MaxSysMem:   ms.Sys,
		Count:       metrics.NewCounter(),
		Rate:        metrics.NewMeter(),
		Latencies: metrics.NewHistogram(
			metrics.NewUniformSample(1024),
		),
	}
}
