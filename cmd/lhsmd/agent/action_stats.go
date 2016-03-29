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

type AgentStats struct {
	sync.Mutex
	stats map[int]*ArchiveStats
}

type ArchiveStats struct {
	changes     uint64
	queueLength metrics.Counter
	completed   metrics.Timer
}

var agentStats *AgentStats

func init() {
	agentStats = &AgentStats{
		stats: make(map[int]*ArchiveStats),
	}
	go func() {
		for {
			time.Sleep(10 * time.Second)
			avail := agentStats.Archives()
			for _, k := range avail {
				archive := agentStats.GetIndex(k)
				changes := atomic.LoadUint64(&archive.changes)
				if changes != 0 {
					atomic.AddUint64(&archive.changes, -changes)
					audit.Logf("archive:%d %s", k, archive)
				}
			}
		}
	}()
}

func (agent *AgentStats) GetIndex(i int) *ArchiveStats {
	agent.Lock()
	defer agent.Unlock()
	s, ok := agent.stats[i]
	if !ok {
		s = &ArchiveStats{
			queueLength: metrics.NewCounter(),
			completed:   metrics.NewTimer(),
		}
		agent.stats[i] = s
	}
	return s
}

func (agent *AgentStats) Archives() (v []int) {
	agent.Lock()
	defer agent.Unlock()
	for k := range agent.stats {
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

func StartAction(a *Action) {
	s := agentStats.GetIndex(int(a.aih.ArchiveID()))
	s.queueLength.Inc(1)
	atomic.AddUint64(&s.changes, 1)
}

func CompleteAction(a *Action, rc int) {
	s := agentStats.GetIndex(int(a.aih.ArchiveID()))
	s.queueLength.Dec(1)
	s.completed.UpdateSince(a.start)
	atomic.AddUint64(&s.changes, 1)
}
