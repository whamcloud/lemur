package agent

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/rcrowley/go-metrics"
	"github.intel.com/hpdd/logging/audit"
)

// ActionStats is a synchronized container for ArchiveStats instances
type ActionStats struct {
	sync.Mutex
	stats map[int]*ArchiveStats
}

// ArchiveStats is a per-archive container of statistics for that backend
type ArchiveStats struct {
	changes     uint64
	queueLength metrics.Counter
	completed   metrics.Timer
}

var actionStats *ActionStats

func init() {
	actionStats = &ActionStats{
		stats: make(map[int]*ArchiveStats),
	}
	go func() {
		for {
			time.Sleep(10 * time.Second)
			avail := actionStats.Archives()
			for _, k := range avail {
				archive := actionStats.GetIndex(k)
				changes := atomic.LoadUint64(&archive.changes)
				if changes != 0 {
					atomic.AddUint64(&archive.changes, -changes)
					audit.Logf("archive:%d %s", k, archive)
				}
			}
		}
	}()
}

// GetIndex returns the *ArchiveStats corresponding to the supplied archive
// number
func (as *ActionStats) GetIndex(i int) *ArchiveStats {
	as.Lock()
	defer as.Unlock()
	s, ok := as.stats[i]
	if !ok {
		s = &ArchiveStats{
			queueLength: metrics.NewCounter(),
			completed:   metrics.NewTimer(),
		}
		metrics.Register(fmt.Sprintf("archive%dCompleted", i), s.completed)
		metrics.Register(fmt.Sprintf("archive%dQueueLength", i), s.queueLength)
		as.stats[i] = s
	}
	return s
}

// Archives returns a slice of archive numbers corresponding to instrumented
// backends
func (as *ActionStats) Archives() (v []int) {
	as.Lock()
	defer as.Unlock()
	for k := range as.stats {
		v = append(v, k)
	}
	return
}

func (s *ArchiveStats) String() string {
	ps := s.completed.Percentiles([]float64{0.5, .75, 0.95, 0.99, 0.999})
	return fmt.Sprintf("total:%v queue:%v %v/%v/%v min:%v max:%v mean:%v median:%v 75%%:%v 95%%:%v 99%%:%v 99.9%%:%v",
		humanize.Comma(s.completed.Count()),
		humanize.Comma(s.queueLength.Count()),
		humanize.Comma(int64(s.completed.Rate1())),
		humanize.Comma(int64(s.completed.Rate5())),
		humanize.Comma(int64(s.completed.Rate15())),
		time.Duration(s.completed.Min()),
		time.Duration(s.completed.Max()),
		time.Duration(int64(s.completed.Mean())),
		time.Duration(int64(ps[0])),
		time.Duration(int64(ps[1])),
		time.Duration(int64(ps[2])),
		time.Duration(int64(ps[3])),
		time.Duration(int64(ps[4])))
}

// StartAction increments stats counters when an action starts
func StartAction(a *Action) {
	s := actionStats.GetIndex(int(a.aih.ArchiveID()))
	s.queueLength.Inc(1)
	atomic.AddUint64(&s.changes, 1)
}

// CompleteAction updates various stats when an action is complete
func CompleteAction(a *Action, rc int) {
	s := actionStats.GetIndex(int(a.aih.ArchiveID()))
	s.queueLength.Dec(1)
	s.completed.UpdateSince(a.start)
	atomic.AddUint64(&s.changes, 1)
}
