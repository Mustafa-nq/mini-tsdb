package storage

import "sync"

// Sample is a single datapoint
type Sample struct {
	Timestamp int64   `json:"ts"`
	Value     float64 `json:"value"`
}

// Series stores samples for a metric name
type Series struct {
	name    string
	samples []Sample
	mu      sync.RWMutex
}

func NewSeries(name string) *Series { return &Series{name: name} }

func (s *Series) Append(ts int64, v float64) {
	s.mu.Lock()
	s.samples = append(s.samples, Sample{Timestamp: ts, Value: v})
	s.mu.Unlock()
}

func (s *Series) Query(from, to int64) []Sample {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// naive linear scan: OK for demo
	out := make([]Sample, 0, 32)
	for _, sm := range s.samples {
		if (from == 0 || sm.Timestamp >= from) && (to == 0 || sm.Timestamp <= to) {
			out = append(out, sm)
		}
	}
	return out
}

func (s *Series) PruneOlderThan(cutoff int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := 0
	for i, sm := range s.samples {
		if sm.Timestamp >= cutoff {
			idx = i
			break
		}
	}
	// if all are older, keep empty slice
	if idx == 0 {
		// if first sample is recent, keep all
		if len(s.samples) > 0 && s.samples[0].Timestamp >= cutoff {
			return
		}
		// else drop all
		s.samples = s.samples[:0]
		return
	}
	s.samples = s.samples[idx:]
}

// Shard contains multiple series with its own lock
type Shard struct {
	mu     sync.RWMutex
	series map[string]*Series
}

func NewShard() *Shard {
	return &Shard{series: make(map[string]*Series)}
}

func (sh *Shard) getOrCreateSeries(name string) *Series {
	sh.mu.RLock()
	s, ok := sh.series[name]
	sh.mu.RUnlock()
	if ok {
		return s
	}
	sh.mu.Lock()
	defer sh.mu.Unlock()
	s, ok = sh.series[name]
	if !ok {
		s = NewSeries(name)
		sh.series[name] = s
	}
	return s
}

func (sh *Shard) AppendSample(name string, ts int64, v float64) {
	s := sh.getOrCreateSeries(name)
	s.Append(ts, v)
}

func (sh *Shard) Query(name string, from, to int64) []Sample {
	sh.mu.RLock()
	s, ok := sh.series[name]
	sh.mu.RUnlock()
	if !ok {
		return nil
	}
	return s.Query(from, to)
}

func (sh *Shard) PruneOlderThan(cutoff int64) {
	sh.mu.RLock()
	list := make([]*Series, 0, len(sh.series))
	for _, s := range sh.series {
		list = append(list, s)
	}
	sh.mu.RUnlock()
	for _, s := range list {
		s.PruneOlderThan(cutoff)
	}
}
