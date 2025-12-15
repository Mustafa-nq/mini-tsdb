package tsdb

import (
	"errors"
	"hash/fnv"
	"mini-tsdb/internal/storage"
	"sync"
	"time"
)

const defaultShards = 16

type Config struct {
	Shards    int
	Retention time.Duration //how long to keep samples

}

func DefaultConfig() Config {
	return Config{
		Shards:    defaultShards,
		Retention: 24 * time.Hour,
	}
}

type TSDB struct {
	shards []*storage.Shard
	cfg    Config
	closed chan struct{}
	wg     sync.WaitGroup
}

func NewTSDB(cfg Config) *TSDB {
	if cfg.Shards <= 0 {
		cfg.Shards = defaultShards
	}
	t := &TSDB{
		cfg: cfg, closed: make(chan struct{}),
	}

	t.shards = make([]*storage.Shard, cfg.Shards)

	for i := 0; i < cfg.Shards; i++ {
		t.shards[i] = storage.NewShard()
	}

	//Start Compactor

	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		compactTicker := time.NewTicker(1 * time.Minute)
		for {
			select {
			case <-compactTicker.C:
				t.compact()
			case <-t.closed:
				compactTicker.Stop()
				return
			}
		}
	}()
	return t
}

func (t *TSDB) Close() {

	close(t.closed)
	t.wg.Wait()
}

func (t *TSDB) hashKey(k string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(k))
	return h.Sum64()
}

func (t *TSDB) shardforKey(k string) *storage.Shard {
	i := t.hashKey(k) % uint64(len(t.shards))
	return t.shards[i]
}

func (t *TSDB) Append(name string, timestamp int64, value float64) error {
	if name == "" {
		return errors.New("metric name empty")
	}
	sh := t.shardforKey(name)
	sh.AppendSample(name, timestamp, value)
	return nil
}

func (t *TSDB) QueryRange(name string, from, to int64) ([]storage.Sample, error) {
	if name == "" {
		return nil, errors.New("metric name empty")
	}
	sh := t.shardforKey(name)
	return sh.Query(name, from, to), nil
}

func (t *TSDB) compact() {
	//retention enforcement: iterate shards and prune samples older than retention
	cutoff := time.Now().Add(-t.cfg.Retention).UnixMilli()
	for _, sh := range t.shards {
		sh.PruneOlderThan(cutoff)
	}
}
