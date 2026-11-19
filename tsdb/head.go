package tsdb

import (
	"math"
	"sort"

	"github.com/builtbyshailendra/minitsdb/chunk"
	"github.com/builtbyshailendra/minitsdb/labels"
)

// memChunk is a full chunk kept in memory.
type memChunk struct {
	mint, maxt int64
	data       []byte
}

// memSeries is one series in the head.
type memSeries struct {
	ref    uint64
	lset   labels.Labels
	chunks []memChunk
	cur    *chunk.Chunk
	curMin int64
	maxT   int64   // last timestamp appended
	lastV  float64 // last value appended
}

func (s *memSeries) append(t int64, v float64) {
	if s.cur == nil || s.cur.Full() {
		if s.cur != nil {
			s.chunks = append(s.chunks, memChunk{mint: s.curMin, maxt: s.maxT, data: append([]byte(nil), s.cur.Bytes()...)})
		}
		s.cur, s.curMin = chunk.New(), t
	}
	s.cur.Append(t, v)
	s.maxT, s.lastV = t, v
}

// samples returns the series' samples within [mint, maxt].
func (s *memSeries) samples(mint, maxt int64) []Point {
	var out []Point
	read := func(data []byte) {
		it := chunk.NewIterator(data)
		for it.Next() {
			t, v := it.At()
			if t >= mint && t <= maxt {
				out = append(out, Point{t, v})
			}
		}
	}
	for _, c := range s.chunks {
		if c.maxt >= mint && c.mint <= maxt {
			read(c.data)
		}
	}
	if s.cur != nil && s.maxT >= mint && s.curMin <= maxt {
		read(s.cur.Bytes())
	}
	return out
}

// head is the in-memory block holding the most recent data.
type head struct {
	series   map[uint64]*memSeries   // by ref
	byHash   map[uint64][]*memSeries // by label hash (a list, for collisions)
	postings map[labels.Label][]uint64
	allRefs  []uint64
	nextRef  uint64
	mint     int64
	maxt     int64
}

func newHead() *head {
	return &head{
		series:   map[uint64]*memSeries{},
		byHash:   map[uint64][]*memSeries{},
		postings: map[labels.Label][]uint64{},
		mint:     math.MaxInt64,
		maxt:     math.MinInt64,
	}
}

func (h *head) get(lset labels.Labels) *memSeries {
	for _, s := range h.byHash[lset.Hash()] {
		if s.lset.Equal(lset) {
			return s
		}
	}
	return nil
}

// add registers a series with a known ref (refs only grow, so postings stay
// sorted by appending).
func (h *head) add(ref uint64, lset labels.Labels) *memSeries {
	s := &memSeries{ref: ref, lset: lset, maxT: math.MinInt64}
	h.series[ref] = s
	hash := lset.Hash()
	h.byHash[hash] = append(h.byHash[hash], s)
	for _, l := range lset {
		h.postings[l] = append(h.postings[l], ref)
	}
	h.allRefs = append(h.allRefs, ref)
	h.nextRef = max(h.nextRef, ref)
	return s
}

func (h *head) appendSample(s *memSeries, t int64, v float64) {
	s.append(t, v)
	h.mint = min(h.mint, t)
	h.maxt = max(h.maxt, t)
}

// selectRefs finds the series matching all matchers: it intersects the
// postings lists of the equality matchers, then filters by the rest.
func (h *head) selectRefs(ms []*labels.Matcher) []uint64 {
	var cand []uint64
	started := false
	for _, m := range ms {
		if m.Type != labels.Equal || m.Value == "" {
			continue
		}
		list := h.postings[labels.Label{Name: m.Name, Value: m.Value}]
		if !started {
			cand, started = append([]uint64(nil), list...), true
		} else {
			cand = intersect(cand, list)
		}
	}
	if !started {
		cand = h.allRefs
	}
	var out []uint64
	for _, ref := range cand {
		if labels.MatchesAll(h.series[ref].lset, ms) {
			out = append(out, ref)
		}
	}
	return out
}

func intersect(a, b []uint64) []uint64 {
	var out []uint64
	for i, j := 0, 0; i < len(a) && j < len(b); {
		switch {
		case a[i] == b[j]:
			out = append(out, a[i])
			i++
			j++
		case a[i] < b[j]:
			i++
		default:
			j++
		}
	}
	return out
}

// numSamples counts samples in memory (for stats).
func (h *head) numSamples() int {
	n := 0
	for _, s := range h.series {
		for _, c := range s.chunks {
			n += chunk.Count(c.data)
		}
		if s.cur != nil {
			n += s.cur.NumSamples()
		}
	}
	return n
}

func sortSeries(ss []Series) {
	sort.Slice(ss, func(i, j int) bool { return labels.Compare(ss[i].Labels, ss[j].Labels) < 0 })
}
