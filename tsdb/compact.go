package tsdb

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/builtbyshailendra/minitsdb/chunk"
	"github.com/builtbyshailendra/minitsdb/crash"
	"github.com/builtbyshailendra/minitsdb/labels"
	"github.com/builtbyshailendra/minitsdb/wal"
)

// Compact does all pending maintenance:
//  1. While the head spans more than 1.5 block ranges, write its oldest
//     range to a level-1 block, drop that data from memory, and checkpoint
//     the WAL so its old segments can be deleted.
//  2. Merge level-1 blocks that share a 3x window into one level-2 block.
//  3. Delete blocks older than the retention period.
func (db *DB) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	for db.head.maxt != math.MinInt64 && db.head.maxt-db.head.mint > db.opts.BlockRange*3/2 {
		start := floorDiv(db.head.mint, db.opts.BlockRange) * db.opts.BlockRange
		if err := db.cutHead(start, start+db.opts.BlockRange); err != nil {
			return err
		}
	}
	if !db.opts.NoMerge {
		if err := db.mergeBlocks(); err != nil {
			return err
		}
	}
	return db.applyRetention()
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && a < 0 {
		q--
	}
	return q
}

// cutHead persists head samples in [mint, maxt) as a block.
func (db *DB) cutHead(mint, maxt int64) error {
	var data []blockSeriesData
	refs := make([]uint64, 0, len(db.head.series))
	for ref := range db.head.series {
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool {
		return labels.Compare(db.head.series[refs[i]].lset, db.head.series[refs[j]].lset) < 0
	})
	for _, ref := range refs {
		s := db.head.series[ref]
		if pts := s.samples(mint, maxt-1); len(pts) > 0 {
			data = append(data, blockSeriesData{lset: s.lset, chunks: encodeChunks(pts)})
		}
	}
	if len(data) > 0 {
		meta, err := writeBlock(db.blockDir(), BlockMeta{MinT: mint, MaxT: maxt, Level: 1}, data)
		if err != nil {
			return err
		}
		if err := db.addBlock(meta.ID); err != nil {
			return err
		}
	}
	db.minValid = max(db.minValid, maxt)
	crash.Point("compact.after-block") // block on disk, head and WAL not yet trimmed

	db.truncateHead(maxt)
	return db.checkpoint()
}

func encodeChunks(pts []Point) []memChunk {
	var out []memChunk
	for i := 0; i < len(pts); i += chunk.MaxSamples {
		end := min(i+chunk.MaxSamples, len(pts))
		c := chunk.New()
		for _, p := range pts[i:end] {
			c.Append(p.T, p.V)
		}
		out = append(out, memChunk{mint: pts[i].T, maxt: pts[end-1].T, data: append([]byte(nil), c.Bytes()...)})
	}
	return out
}

// truncateHead drops samples before t from memory. Series left with no
// samples are forgotten (series churn would otherwise grow memory forever).
func (db *DB) truncateHead(t int64) {
	old := db.head
	h := newHead()
	h.nextRef = old.nextRef
	refs := make([]uint64, 0, len(old.series))
	for ref := range old.series {
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i] < refs[j] })
	for _, ref := range refs {
		s := old.series[ref]
		pts := s.samples(t, math.MaxInt64)
		if len(pts) == 0 {
			continue
		}
		ns := h.add(ref, s.lset)
		for _, p := range pts {
			h.appendSample(ns, p.T, p.V)
		}
	}
	h.nextRef = old.nextRef
	db.head = h
}

// checkpoint writes the head's whole content to wal/checkpoint.N (N = the
// first segment after it), then deletes older segments and checkpoints.
// Replay reads the newest checkpoint, then segments from N on.
func (db *DB) checkpoint() error {
	n, err := db.wal.Cut()
	if err != nil {
		return err
	}
	walDir := filepath.Join(db.opts.Dir, "wal")
	tmp := filepath.Join(walDir, fmt.Sprintf("checkpoint.%08d.tmp", n))
	_ = os.RemoveAll(tmp)
	cw, err := wal.Open(tmp, math.MaxInt64)
	if err != nil {
		return err
	}
	var series []wal.RefSeries
	var samples []wal.RefSample
	refs := make([]uint64, 0, len(db.head.series))
	for ref := range db.head.series {
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i] < refs[j] })
	for _, ref := range refs {
		s := db.head.series[ref]
		series = append(series, wal.RefSeries{Ref: ref, Labels: s.lset})
		for _, p := range s.samples(math.MinInt64, math.MaxInt64) {
			samples = append(samples, wal.RefSample{Ref: ref, T: p.T, V: p.V})
		}
	}
	recs := []wal.Record{{Type: wal.RecSeries, Payload: wal.EncodeSeries(series)}}
	if len(samples) > 0 {
		recs = append(recs, wal.Record{Type: wal.RecSamples, Payload: wal.EncodeSamples(samples)})
	}
	if err := cw.Log(recs...); err != nil {
		cw.Close()
		return err
	}
	if err := cw.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(walDir, fmt.Sprintf("checkpoint.%08d", n))); err != nil {
		return err
	}
	crash.Point("compact.after-checkpoint") // old segments not yet deleted
	return cleanWAL(walDir, n)
}

// cleanWAL removes segments before n and checkpoints older than n.
func cleanWAL(walDir string, n int) error {
	if err := wal.Truncate(walDir, n); err != nil {
		return err
	}
	ents, _ := os.ReadDir(walDir)
	for _, e := range ents {
		var k int
		if _, err := fmt.Sscanf(e.Name(), "checkpoint.%08d", &k); err == nil && (k < n || strings.HasSuffix(e.Name(), ".tmp")) {
			if err := os.RemoveAll(filepath.Join(walDir, e.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

// latestCheckpoint returns the newest complete checkpoint's number (0 if none).
func latestCheckpoint(walDir string) int {
	ents, _ := os.ReadDir(walDir)
	best := 0
	for _, e := range ents {
		var k int
		if strings.HasSuffix(e.Name(), ".tmp") {
			continue
		}
		if _, err := fmt.Sscanf(e.Name(), "checkpoint.%08d", &k); err == nil && k > best {
			best = k
		}
	}
	return best
}

// mergeBlocks merges level-1 blocks in the same 3-range window, once the
// head has moved past that window (no more blocks will arrive in it).
// MergeBlocks runs only the merge step.
func (db *DB) MergeBlocks() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.mergeBlocks()
}

func (db *DB) mergeBlocks() error {
	window := 3 * db.opts.BlockRange
	groups := map[int64][]*Block{}
	headStart := floorDiv(db.head.mint, db.opts.BlockRange) * db.opts.BlockRange
	for _, b := range db.blocks {
		w := floorDiv(b.Meta.MinT, window) * window
		if b.Meta.Level == 1 && w+window <= headStart {
			groups[w] = append(groups[w], b)
		}
	}
	keys := make([]int64, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for _, w := range keys {
		if len(groups[w]) < 2 {
			continue
		}
		if err := db.merge(groups[w], w, w+window); err != nil {
			return err
		}
	}
	return nil
}

// merge writes one block with the series of all given blocks. Chunks are
// copied as they are, not re-encoded.
func (db *DB) merge(src []*Block, mint, maxt int64) error {
	type acc struct {
		lset   labels.Labels
		chunks []memChunk
	}
	byKey := map[string]*acc{}
	meta := BlockMeta{MinT: mint, MaxT: maxt, Level: 2}
	for _, b := range src {
		meta.Sources = append(meta.Sources, b.Meta.Sources...)
		for _, s := range b.series {
			k := s.lset.String()
			a := byKey[k]
			if a == nil {
				a = &acc{lset: s.lset}
				byKey[k] = a
			}
			for _, r := range s.chunks {
				data, err := b.readChunk(r)
				if err != nil {
					return err
				}
				a.chunks = append(a.chunks, memChunk{mint: r.mint, maxt: r.maxt, data: data})
			}
		}
	}
	var data []blockSeriesData
	for _, a := range byKey {
		sort.Slice(a.chunks, func(i, j int) bool { return a.chunks[i].mint < a.chunks[j].mint })
		data = append(data, blockSeriesData{lset: a.lset, chunks: a.chunks})
	}
	sort.Slice(data, func(i, j int) bool { return labels.Compare(data[i].lset, data[j].lset) < 0 })
	sort.Strings(meta.Sources)
	nm, err := writeBlock(db.blockDir(), meta, data)
	if err != nil {
		return err
	}
	crash.Point("compact.after-merge") // new block in place, sources not deleted
	if err := db.addBlock(nm.ID); err != nil {
		return err
	}
	return db.removeSuperseded()
}

func (db *DB) applyRetention() error {
	if db.opts.Retention <= 0 || len(db.blocks) == 0 {
		return nil
	}
	newest := db.blocks[len(db.blocks)-1].Meta.MaxT
	if db.head.maxt != math.MinInt64 {
		newest = max(newest, db.head.maxt)
	}
	var keep []*Block
	for _, b := range db.blocks {
		if b.Meta.MaxT <= newest-db.opts.Retention {
			b.Close()
			if err := os.RemoveAll(b.dir); err != nil {
				return err
			}
			db.Deleted++
			continue
		}
		keep = append(keep, b)
	}
	db.blocks = keep
	return nil
}
