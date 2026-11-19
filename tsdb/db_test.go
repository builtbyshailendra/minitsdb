package tsdb

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/builtbyshailendra/minitsdb/labels"
	"github.com/builtbyshailendra/minitsdb/wal"
)

func open(t testing.TB, dir string) *DB {
	t.Helper()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func ls(kv ...string) labels.Labels { return labels.FromStrings(kv...) }

func eq(name, value string) *labels.Matcher { return labels.MustMatcher(labels.Equal, name, value) }

func TestAppendRulesAndSelect(t *testing.T) {
	db := open(t, t.TempDir())
	a := ls("__name__", "http_requests_total", "code", "200", "job", "api")
	b := ls("__name__", "http_requests_total", "code", "500", "job", "api")
	c := ls("__name__", "up", "job", "api")
	res, err := db.Append([]Sample{{a, 1000, 1}, {b, 1000, 0}, {c, 1000, 1}, {a, 2000, 3}, {b, 2000, 1}})
	if err != nil || res.Accepted != 5 {
		t.Fatalf("%+v %v", res, err)
	}
	res, _ = db.Append([]Sample{
		{a, 2000, 3},           // exact repeat of the newest: ignored, not an error
		{a, 2000, 4},           // same time, different value
		{a, 1500, 9},           // older than the newest
		{ls("job", "x"), 1, 1}, // no metric name
		{a, 3000, 6},           // fine
	})
	if res.Accepted != 1 || res.Ignored != 1 || res.Rejected != 3 || !errors.Is(res.FirstErr, ErrDuplicate) {
		t.Fatalf("%+v", res)
	}

	cases := []struct {
		ms   []*labels.Matcher
		want int
	}{
		{[]*labels.Matcher{eq("__name__", "http_requests_total")}, 2},
		{[]*labels.Matcher{eq("__name__", "http_requests_total"), eq("code", "500")}, 1},
		{[]*labels.Matcher{eq("job", "api"), labels.MustMatcher(labels.NotEqual, "code", "200")}, 2}, // code 500, and up (no code)
		{[]*labels.Matcher{labels.MustMatcher(labels.Regexp, "code", "2..")}, 1},
		{[]*labels.Matcher{labels.MustMatcher(labels.Regexp, "code", "2")}, 0}, // anchored
		{[]*labels.Matcher{eq("__name__", "http_requests_total"), labels.MustMatcher(labels.NotRegexp, "code", "5..")}, 1},
	}
	for _, tc := range cases {
		if got := db.Select(math.MinInt64, math.MaxInt64, tc.ms...); len(got) != tc.want {
			t.Errorf("%v: %d series, want %d", tc.ms, len(got), tc.want)
		}
	}
	got := db.Select(1500, 2500, eq("code", "200"))
	if len(got) != 1 || len(got[0].Points) != 1 || got[0].Points[0] != (Point{2000, 3}) {
		t.Fatalf("time range: %+v", got)
	}
}

func TestRestartReplaysTheLog(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 500 { // 500 samples per series: several full chunks
		var batch []Sample
		for s := range 20 {
			batch = append(batch, Sample{ls("__name__", "m", "s", fmt.Sprint(s)), int64(i) * 15000, float64(i * s)})
		}
		if _, err := db.Append(batch); err != nil {
			t.Fatal(err)
		}
	}
	before := db.Select(math.MinInt64, math.MaxInt64, eq("__name__", "m"))
	_ = db.Close()

	db2 := open(t, dir)
	after := db2.Select(math.MinInt64, math.MaxInt64, eq("__name__", "m"))
	if len(after) != 20 || fmt.Sprint(after) != fmt.Sprint(before) {
		t.Fatalf("replayed %d series; differs: %v", len(after), fmt.Sprint(after) != fmt.Sprint(before))
	}
	if db2.Replay.Records != 501 || db2.Replay.TornBytes != 0 {
		t.Fatalf("replay %+v", db2.Replay)
	}
}

// A crash in the middle of a write leaves half a record at the end. Replay
// cuts it off and the next writes go after the good data.
func TestTornTailIsCutOff(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(Options{Dir: dir})
	for i := range 10 {
		_, _ = db.Append([]Sample{{ls("__name__", "m"), int64(i), float64(i)}})
	}
	_ = db.Close()
	seg := filepath.Join(dir, "wal", "00000001")
	good, _ := os.Stat(seg)
	// Half of a further record: a header promising 100 bytes, then 20.
	f, _ := os.OpenFile(seg, os.O_APPEND|os.O_WRONLY, 0)
	_, _ = f.Write(append([]byte{wal.RecSamples, 0, 0, 0, 100, 1, 2, 3, 4}, make([]byte, 20)...))
	_ = f.Close()

	db2 := open(t, dir)
	if db2.Replay.TornBytes != 29 {
		t.Fatalf("replay %+v", db2.Replay)
	}
	if st, _ := os.Stat(seg); st.Size() != good.Size() {
		t.Fatalf("segment is %d bytes, want %d", st.Size(), good.Size())
	}
	if _, err := db2.Append([]Sample{{ls("__name__", "m"), 10, 10}}); err != nil {
		t.Fatal(err)
	}
	_ = db2.Close()
	db3 := open(t, dir)
	if got := db3.Select(0, 100, eq("__name__", "m")); len(got[0].Points) != 11 {
		t.Fatalf("%d points after repair and append", len(got[0].Points))
	}
}

// A flipped bit in the middle of the log: everything from that record on
// is dropped (it can't be trusted, and nothing after it was acknowledged
// in a crash scenario). Replay reports what it dropped.
func TestCorruptRecordDropsTheRest(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(Options{Dir: dir, SegmentSize: 200})
	for i := range 40 {
		_, _ = db.Append([]Sample{{ls("__name__", "m"), int64(i), float64(i)}})
	}
	_ = db.Close()
	segs, _ := wal.Segments(filepath.Join(dir, "wal"))
	if len(segs) < 4 {
		t.Fatalf("want several segments, got %v", segs)
	}
	p := filepath.Join(dir, "wal", fmt.Sprintf("%08d", segs[1]))
	b, _ := os.ReadFile(p)
	b[len(b)/2] ^= 0x10
	_ = os.WriteFile(p, b, 0o644)

	db2 := open(t, dir)
	n := len(db2.Select(0, 100, eq("__name__", "m"))[0].Points)
	t.Logf("replay %+v: %d of 40 samples kept", db2.Replay, n)
	if db2.Replay.Dropped != len(segs)-2 || n >= 40 || n == 0 {
		t.Fatalf("replay %+v, %d samples", db2.Replay, n)
	}
}

// How much the fsync per write costs, and why writes come in batches.
func TestAppendThroughput(t *testing.T) {
	for _, tc := range []struct {
		batch  int
		noSync bool
	}{{1, false}, {100, false}, {1000, false}, {1000, true}} {
		db := open(t, t.TempDir())
		db.wal.NoSync = tc.noSync
		total := 20_000
		if tc.batch == 1 {
			total = 2_000 // one fsync per sample is slow
		}
		lsets := make([]labels.Labels, 1000)
		for i := range lsets {
			lsets[i] = ls("__name__", "m", "s", fmt.Sprint(i))
		}
		start := time.Now()
		n := 0
		for i := 0; n < total; i++ {
			batch := make([]Sample, tc.batch)
			for j := range batch {
				batch[j] = Sample{lsets[(n+j)%len(lsets)], int64(i) * 15000, float64(n + j)}
			}
			if _, err := db.Append(batch); err != nil {
				t.Fatal(err)
			}
			n += tc.batch
		}
		el := time.Since(start)
		sync := "fsync each write"
		if tc.noSync {
			sync = "no fsync"
		}
		t.Logf("batches of %4d, %s: %8.0f samples/s (%v per write)", tc.batch, sync, float64(n)/el.Seconds(), (el / time.Duration(n/tc.batch)).Round(time.Microsecond))
	}
}
