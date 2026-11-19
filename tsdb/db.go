// Package tsdb ties the pieces together: the write-ahead log, the in-memory
// head, and immutable blocks on disk.
package tsdb

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
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
	BlockRange  int64 // milliseconds per level-1 block, default 2 hours
	Retention   int64 // delete blocks older than this (ms); 0 keeps everything
	NoMerge     bool  // skip merging blocks (for measuring what merging does)
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
	// ErrOutOfBounds: older than the head; that time range is already a block.
	ErrOutOfBounds = errors.New("sample older than the head (already in a block)")
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
	opts     Options
	mu       sync.RWMutex
	head     *head
	wal      *wal.WAL
	blocks   []*Block // sorted by MinT
	minValid int64    // samples before this are in blocks; replay skips them
	Replay   wal.ReplayStats
	Deleted  int // blocks removed by retention
	Cleaned  int // leftover .tmp or superseded blocks removed at open
}

func (db *DB) blockDir() string { return filepath.Join(db.opts.Dir, "blocks") }

// Open opens (or creates) a database: it loads the blocks, then replays the
// newest WAL checkpoint and the segments after it into the head.
func Open(opts Options) (*DB, error) {
	if opts.BlockRange == 0 {
		opts.BlockRange = 2 * 3600 * 1000
	}
	db := &DB{opts: opts, head: newHead(), minValid: math.MinInt64}
	if err := os.MkdirAll(db.blockDir(), 0o755); err != nil {
		return nil, err
	}
	ents, err := os.ReadDir(db.blockDir())
	if err != nil {
		return nil, err
	}
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".tmp") { // a block write that crashed
			if err := os.RemoveAll(filepath.Join(db.blockDir(), e.Name())); err != nil {
				return nil, err
			}
			db.Cleaned++
			continue
		}
		if err := db.addBlock(e.Name()); err != nil {
			return nil, err
		}
	}
	if err := db.removeSuperseded(); err != nil {
		return nil, err
	}
	for _, b := range db.blocks {
		db.minValid = max(db.minValid, b.Meta.MaxT)
	}

	walDir := filepath.Join(opts.Dir, "wal")
	cp := latestCheckpoint(walDir)
	if cp > 0 {
		if _, err := wal.Replay(filepath.Join(walDir, fmt.Sprintf("checkpoint.%08d", cp)), 0, db.replayRecord); err != nil {
			return nil, fmt.Errorf("checkpoint: %w", err)
		}
		if err := cleanWAL(walDir, cp); err != nil {
			return nil, err
		}
	}
	st, err := wal.Replay(walDir, cp, db.replayRecord)
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
			if ms == nil || s.T <= ms.maxT || s.T < db.minValid {
				continue // unknown series, already have it, or already in a block
			}
			db.head.appendSample(ms, s.T, s.V)
		}
	}
	return nil
}

// Close closes the write-ahead log and the blocks.
func (db *DB) Close() error {
	for _, b := range db.blocks {
		b.Close()
	}
	return db.wal.Close()
}

func (db *DB) addBlock(id string) error {
	b, err := openBlock(filepath.Join(db.blockDir(), id))
	if err != nil {
		return err
	}
	db.blocks = append(db.blocks, b)
	sort.Slice(db.blocks, func(i, j int) bool { return db.blocks[i].Meta.MinT < db.blocks[j].Meta.MinT })
	return nil
}

// removeSuperseded deletes blocks whose sources are all inside another,
// larger block: a merge that crashed before deleting what it merged.
func (db *DB) removeSuperseded() error {
	var keep []*Block
	for _, b := range db.blocks {
		covered := false
		for _, o := range db.blocks {
			if o != b && len(o.Meta.Sources) > len(b.Meta.Sources) && containsAll(o.Meta.Sources, b.Meta.Sources) {
				covered = true
			}
		}
		if covered {
			b.Close()
			if err := os.RemoveAll(b.dir); err != nil {
				return err
			}
			db.Cleaned++
			continue
		}
		keep = append(keep, b)
	}
	db.blocks = keep
	return nil
}

func containsAll(big, small []string) bool {
	set := map[string]bool{}
	for _, s := range big {
		set[s] = true
	}
	for _, s := range small {
		if !set[s] {
			return false
		}
	}
	return true
}

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
		case s.T < db.minValid:
			reject(ErrOutOfBounds)
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
	byKey := map[string]*Series{}
	add := func(lset labels.Labels, pts []Point) {
		if len(pts) == 0 {
			return
		}
		k := lset.String()
		s := byKey[k]
		if s == nil {
			s = &Series{Labels: lset}
			byKey[k] = s
		}
		s.Points = append(s.Points, pts...)
	}
	for _, b := range db.blocks { // oldest first
		if b.Meta.MaxT <= mint || b.Meta.MinT > maxt {
			continue
		}
		for _, id := range b.selectIDs(ms) {
			pts, err := b.samples(id, mint, maxt)
			if err == nil {
				add(b.series[id].lset, pts)
			}
		}
	}
	for _, ref := range db.head.selectRefs(ms) {
		s := db.head.series[ref]
		add(s.lset, s.samples(mint, maxt))
	}
	out := make([]Series, 0, len(byKey))
	for _, s := range byKey {
		sort.SliceStable(s.Points, func(i, j int) bool { return s.Points[i].T < s.Points[j].T })
		out = append(out, *s)
	}
	sortSeries(out)
	return out
}

// Stats summarizes the head.
type Stats struct {
	HeadSeries   int
	HeadSamples  int
	HeadMinT     int64
	HeadMaxT     int64
	Blocks       []BlockMeta
	IndexBytes   int64
	ChunkBytes   int64
	BlockSamples int
}

// Stats returns counts for the head and the blocks.
func (db *DB) Stats() Stats {
	db.mu.RLock()
	defer db.mu.RUnlock()
	st := Stats{HeadSeries: len(db.head.series), HeadSamples: db.head.numSamples(), HeadMinT: db.head.mint, HeadMaxT: db.head.maxt}
	for _, b := range db.blocks {
		st.Blocks = append(st.Blocks, b.Meta)
		i, c := b.Size()
		st.IndexBytes += i
		st.ChunkBytes += c
		st.BlockSamples += b.Meta.Samples
	}
	return st
}
