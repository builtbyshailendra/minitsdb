// Package tsdb ties the pieces together: the write-ahead log, the in-memory
// head, and (from step 3) immutable blocks on disk.
package tsdb

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/builtbyshailendra/minitsdb/crash"
	"github.com/builtbyshailendra/minitsdb/labels"
	"github.com/builtbyshailendra/minitsdb/wal"
)

// Options configure a DB.
type Options struct {
	Dir         string
	SegmentSize int64 // WAL segment size, default 32 MiB
	NoSync      bool  // don't fsync the WAL (unsafe; for measuring the cost)
}

// Sample is one value of one series.
type Sample struct {
	Labels labels.Labels
	T      int64 // milliseconds since the Unix epoch
	V      float64
}

// Point is a timestamp and value.
type Point struct {
	T int64
	V float64
}

// Series is a query result.
type Series struct {
	Labels labels.Labels
	Points []Point
}

var (
	// ErrOutOfOrder: a sample older than the newest one for its series.
	ErrOutOfOrder = errors.New("out-of-order sample")
	// ErrDuplicate: same timestamp as the newest sample, different value.
	ErrDuplicate = errors.New("duplicate sample for timestamp")
	// ErrNoName: every series needs a metric name.
	ErrNoName = errors.New("series without __name__")
)

// AppendResult reports what an Append did.
type AppendResult struct {
	Accepted int
	Ignored  int // exact repeats of the newest sample (a retried write)
	Rejected int
	FirstErr error // the first reason a sample was rejected
}

// DB is a time-series database.
type DB struct {
	opts   Options
	mu     sync.RWMutex
	head   *head
	wal    *wal.WAL
	Replay wal.ReplayStats // what Open recovered
}

// Open opens (or creates) a database and replays its write-ahead log.
func Open(opts Options) (*DB, error) {
	db := &DB{opts: opts, head: newHead()}
	walDir := filepath.Join(opts.Dir, "wal")
	st, err := wal.Replay(walDir, 0, db.replayRecord)
	if err != nil {
		return nil, fmt.Errorf("replay: %w", err)
	}
	db.Replay = st
	if db.wal, err = wal.Open(walDir, opts.SegmentSize); err != nil {
		return nil, err
	}
	db.wal.NoSync = opts.NoSync
	return db, nil
}

func (db *DB) replayRecord(typ byte, payload []byte) error {
	switch typ {
	case wal.RecSeries:
		ss, err := wal.DecodeSeries(payload)
		if err != nil {
			return err
		}
		for _, s := range ss {
			if db.head.series[s.Ref] == nil {
				db.head.add(s.Ref, s.Labels)
			}
		}
	case wal.RecSamples:
		ss, err := wal.DecodeSamples(payload)
		if err != nil {
			return err
		}
		for _, s := range ss {
			ms := db.head.series[s.Ref]
			if ms == nil || s.T <= ms.maxT {
				continue // unknown series, or already have it
			}
			db.head.appendSample(ms, s.T, s.V)
		}
	}
	return nil
}

// Close closes the write-ahead log.
func (db *DB) Close() error { return db.wal.Close() }

// Append writes a batch. Each sample is accepted or rejected on its own;
// the accepted ones are logged as one write and fsynced before Append
// returns, so a nil error means they survive a crash.
func (db *DB) Append(batch []Sample) (AppendResult, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	var res AppendResult
	reject := func(err error) {
		res.Rejected++
		if res.FirstErr == nil {
			res.FirstErr = err
		}
	}
	type pend struct {
		ref    uint64
		lset   labels.Labels
		maxT   int64
		lastV  float64
		exists *memSeries
	}
	pending := map[uint64][]*pend{} // label hash -> series touched by this batch
	var newSeries []wal.RefSeries
	var samples []wal.RefSample
	nextRef := db.head.nextRef

	for _, s := range batch {
		if s.Labels.Get(labels.MetricName) == "" {
			reject(ErrNoName)
			continue
		}
		hash := s.Labels.Hash()
		var p *pend
		for _, x := range pending[hash] {
			if x.lset.Equal(s.Labels) {
				p = x
			}
		}
		if p == nil {
			if ms := db.head.get(s.Labels); ms != nil {
				p = &pend{ref: ms.ref, lset: ms.lset, maxT: ms.maxT, lastV: ms.lastV, exists: ms}
			} else {
				nextRef++
				p = &pend{ref: nextRef, lset: s.Labels, maxT: -1 << 63}
				newSeries = append(newSeries, wal.RefSeries{Ref: p.ref, Labels: s.Labels})
			}
			pending[hash] = append(pending[hash], p)
		}
		switch {
		case s.T == p.maxT && s.V == p.lastV:
			res.Ignored++ // a retry of what we already have
		case s.T == p.maxT:
			reject(ErrDuplicate)
		case s.T < p.maxT:
			reject(ErrOutOfOrder)
		default:
			samples = append(samples, wal.RefSample{Ref: p.ref, T: s.T, V: s.V})
			p.maxT, p.lastV = s.T, s.V
			res.Accepted++
		}
	}

	var recs []wal.Record
	if len(newSeries) > 0 {
		recs = append(recs, wal.Record{Type: wal.RecSeries, Payload: wal.EncodeSeries(newSeries)})
	}
	if len(samples) > 0 {
		recs = append(recs, wal.Record{Type: wal.RecSamples, Payload: wal.EncodeSamples(samples)})
	}
	if len(recs) == 0 {
		return res, nil
	}
	// Durable first, then visible.
	if err := db.wal.Log(recs...); err != nil {
		return AppendResult{}, err
	}
	crash.Point("head.after-wal")
	for _, s := range newSeries {
		db.head.add(s.Ref, s.Labels)
	}
	for _, s := range samples {
		db.head.appendSample(db.head.series[s.Ref], s.T, s.V)
	}
	return res, nil
}

// Select returns every series matching all matchers, with its samples in
// [mint, maxt], sorted by labels.
func (db *DB) Select(mint, maxt int64, ms ...*labels.Matcher) []Series {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var out []Series
	for _, ref := range db.head.selectRefs(ms) {
		s := db.head.series[ref]
		if pts := s.samples(mint, maxt); len(pts) > 0 {
			out = append(out, Series{Labels: s.lset, Points: pts})
		}
	}
	sortSeries(out)
	return out
}

// Stats summarizes the head.
type Stats struct {
	Series  int
	Samples int
	MinT    int64
	MaxT    int64
}

// Stats returns counts for the head.
func (db *DB) Stats() Stats {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return Stats{Series: len(db.head.series), Samples: db.head.numSamples(), MinT: db.head.mint, MaxT: db.head.maxt}
}
