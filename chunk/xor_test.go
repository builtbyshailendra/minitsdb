package chunk

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"strings"
	"testing"
)

type sample struct {
	t int64
	v float64
}

func encode(ss []sample) []byte {
	c := New()
	for _, s := range ss {
		c.Append(s.t, s.v)
	}
	return append([]byte(nil), c.Bytes()...)
}

func decode(t testing.TB, b []byte) []sample {
	t.Helper()
	var out []sample
	it := NewIterator(b)
	for it.Next() {
		ts, v := it.At()
		out = append(out, sample{ts, v})
	}
	if it.Err() != nil {
		t.Fatalf("decode: %v", it.Err())
	}
	return out
}

// same compares values bit for bit, so NaN payloads and -0 must survive.
func same(t testing.TB, want, got []sample) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("decoded %d samples, want %d", len(got), len(want))
	}
	for i := range want {
		if want[i].t != got[i].t || math.Float64bits(want[i].v) != math.Float64bits(got[i].v) {
			t.Fatalf("sample %d: got (%d, %v), want (%d, %v)", i, got[i].t, got[i].v, want[i].t, want[i].v)
		}
	}
}

func TestRoundTripEdgeCases(t *testing.T) {
	stale := math.Float64frombits(0x7ff0000000000002) // Prometheus's staleness marker, a NaN
	cases := map[string][]sample{
		"one sample":     {{1700000000000, 1}},
		"two samples":    {{0, 0}, {15000, 1}},
		"negative times": {{-30000, 1}, {-15000, 2}, {0, 3}},
		"special floats": {{0, math.NaN()}, {1, math.Inf(1)}, {2, math.Inf(-1)}, {3, math.Copysign(0, -1)}, {4, 0}, {5, stale}, {6, math.MaxFloat64}, {7, math.SmallestNonzeroFloat64}},
		"every bucket":   {{0, 1}, {1000, 1}, {2000, 1}, {3001, 1}, {12000, 1}, {100000, 1}, {700000, 1}, {1 << 40, 1}, {1<<40 + 1, 1}},
		"bucket edges":   {{0, 1}, {10000, 1}, {28192, 1}, {38193, 1}, {56385 - 8191, 1}},
		"same value 120x": func() []sample {
			var ss []sample
			for i := range MaxSamples {
				ss = append(ss, sample{int64(i) * 15000, 42})
			}
			return ss
		}(),
	}
	for name, ss := range cases {
		t.Run(name, func(t *testing.T) { same(t, ss, decode(t, encode(ss))) })
	}
}

// FuzzRoundTrip: any increasing timestamps and any float bits must decode
// to exactly what was encoded.
func FuzzRoundTrip(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	f.Add(make([]byte, 64))
	f.Fuzz(func(t *testing.T, data []byte) {
		var ss []sample
		ts := int64(0)
		for len(data) >= 10 && len(ss) < MaxSamples {
			gap := int64(binary.LittleEndian.Uint16(data)) // 0..65535 ms
			if data[0]&1 == 1 {
				gap <<= 16 // sometimes a large jump
			}
			ts += gap + 1
			ss = append(ss, sample{ts, math.Float64frombits(binary.LittleEndian.Uint64(data[2:]))})
			data = data[10:]
		}
		if len(ss) == 0 {
			return
		}
		same(t, ss, decode(t, encode(ss)))
	})
}

func TestTruncatedChunkIsAnErrorNotAPanic(t *testing.T) {
	var ss []sample
	for i := range 50 {
		ss = append(ss, sample{int64(i) * 15000, float64(i) * 1.5})
	}
	b := encode(ss)
	for cut := 2; cut < len(b); cut++ {
		it := NewIterator(b[:cut])
		n := 0
		for it.Next() {
			n++
		}
		if it.Err() == nil && n == 50 {
			t.Fatalf("cut at %d of %d bytes decoded everything", cut, len(b))
		}
	}
}

// series generators for the compression measurements.
type gen struct {
	name string
	next func(rng *rand.Rand, i int, prevT int64, prevV float64) (int64, float64)
}

func regular(interval int64) func(*rand.Rand, int, int64) int64 {
	return func(_ *rand.Rand, i int, prev int64) int64 { return prev + interval }
}

var gens = []gen{
	{"constant (up, a flag)", func(_ *rand.Rand, _ int, t int64, _ float64) (int64, float64) { return t + 15000, 1 }},
	{"counter, +0..10 per scrape", func(r *rand.Rand, _ int, t int64, v float64) (int64, float64) {
		return t + 15000, v + float64(r.IntN(11))
	}},
	{"counter, busy (+0..1000)", func(r *rand.Rand, _ int, t int64, v float64) (int64, float64) {
		return t + 15000, v + float64(r.IntN(1001))
	}},
	{"gauge, 2 decimals (CPU %)", func(r *rand.Rand, _ int, t int64, v float64) (int64, float64) {
		x := math.Round((v+r.NormFloat64()*2)*100) / 100
		return t + 15000, math.Max(0, math.Min(100, x))
	}},
	{"gauge, memory bytes (changes by pages)", func(r *rand.Rand, _ int, t int64, v float64) (int64, float64) {
		return t + 15000, v + float64(r.IntN(201)-100)*4096
	}},
	{"counter, scrape jitter ±5 ms", func(r *rand.Rand, _ int, t int64, v float64) (int64, float64) {
		return t + 15000 + int64(r.IntN(11)-5), v + float64(r.IntN(11))
	}},
	{"random float64 (worst case)", func(r *rand.Rand, _ int, t int64, _ float64) (int64, float64) {
		return t + 15000, r.Float64() * 1e6
	}},
}

// TestCompression measures bytes per sample over many full chunks of each
// kind of series. The Gorilla paper reports 1.37 bytes per sample on
// Facebook's production data; these series are synthetic.
func TestCompression(t *testing.T) {
	var tbl strings.Builder
	tbl.WriteString("| Series (15 s scrape interval) | Bytes per sample | vs 16 raw bytes |\n|---|---|---|\n")
	for _, g := range gens {
		rng := rand.New(rand.NewPCG(1, 2))
		const chunks = 200
		var bytes, samples int
		ts, v := int64(1_700_000_000_000), 1000.0
		for range chunks {
			var ss []sample
			for i := range MaxSamples {
				ts, v = g.next(rng, i, ts, v)
				ss = append(ss, sample{ts, v})
			}
			b := encode(ss)
			same(t, ss, decode(t, b))
			bytes += len(b)
			samples += len(ss)
		}
		bps := float64(bytes) / float64(samples)
		fmt.Fprintf(&tbl, "| %s | %.2f | %.1fx smaller |\n", g.name, bps, 16/bps)
	}
	t.Log("\n" + tbl.String())
}

func BenchmarkAppend(b *testing.B) {
	rng := rand.New(rand.NewPCG(1, 2))
	for b.Loop() {
		c := New()
		v := 0.0
		for i := range MaxSamples {
			v += float64(rng.IntN(11))
			c.Append(int64(i)*15000, v)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*MaxSamples), "ns/sample")
}

func BenchmarkIterate(b *testing.B) {
	c := New()
	v := 0.0
	for i := range MaxSamples {
		v += float64(i % 7)
		c.Append(int64(i)*15000, v)
	}
	data := c.Bytes()
	for b.Loop() {
		it := NewIterator(data)
		for it.Next() {
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*MaxSamples), "ns/sample")
}
