package tsdb

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/builtbyshailendra/minitsdb/labels"
)

const (
	hour     = int64(3600 * 1000)
	interval = int64(15000)
)

// load writes `hours` of data for n series at a 15 s interval, compacting
// after every simulated hour. Values are counters that can be recomputed.
func load(t testing.TB, db *DB, n int, hours int64) int {
	t.Helper()
	lsets := make([]labels.Labels, n)
	for i := range lsets {
		lsets[i] = ls("__name__", "requests_total", "instance", fmt.Sprintf("host-%03d", i), "job", "api")
	}
	total := 0
	for ts := int64(0); ts < hours*hour; ts += interval {
		batch := make([]Sample, n)
		for i := range batch {
			batch[i] = Sample{lsets[i], ts, value(i, ts)}
		}
		if _, err := db.Append(batch); err != nil {
			t.Fatal(err)
		}
		total += n
		if (ts+interval)%hour == 0 {
			if err := db.Compact(); err != nil {
				t.Fatal(err)
			}
		}
	}
	return total
}

func value(series int, ts int64) float64 { return float64((ts / interval) * int64(series%7+1)) }

// verify reads everything back and checks every sample.
func verify(t testing.TB, db *DB, n int, hours int64) {
	t.Helper()
	got := db.Select(math.MinInt64, math.MaxInt64, eq("__name__", "requests_total"))
	if len(got) != n {
		t.Fatalf("%d series, want %d", len(got), n)
	}
	want := int(hours * hour / interval)
	for _, s := range got {
		var i int
		fmt.Sscanf(s.Labels.Get("instance"), "host-%03d", &i)
		if len(s.Points) != want {
			t.Fatalf("%v: %d points, want %d", s.Labels, len(s.Points), want)
		}
		for k, p := range s.Points {
			if p.T != int64(k)*interval || p.V != value(i, p.T) {
				t.Fatalf("%v point %d: %+v", s.Labels, k, p)
			}
		}
	}
}

func dirSize(t testing.TB, dir string) int64 {
	var n int64
	_ = filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			n += info.Size()
		}
		return nil
	})
	return n
}

// 24 hours of 200 series: the head cuts 2-hour blocks, blocks merge into
// 6-hour ones, the WAL is checkpointed, and everything reads back exactly,
// before and after a restart.
func TestBlocksCompactionAndRestart(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	const n, hours = 200, 24
	start := time.Now()
	total := load(t, db, n, hours)
	took := time.Since(start)
	verify(t, db, n, hours)

	st := db.Stats()
	levels := map[int]int{}
	for _, b := range st.Blocks {
		levels[b.Level]++
	}
	blocks := dirSize(t, filepath.Join(dir, "blocks"))
	walBytes := dirSize(t, filepath.Join(dir, "wal"))
	t.Logf("%d samples in %v (%.0f/s, no fsync); blocks by level %v; head keeps %d samples (%.1f h)",
		total, took.Round(time.Millisecond), float64(total)/took.Seconds(), levels, st.HeadSamples, float64(st.HeadMaxT-st.HeadMinT)/float64(hour))
	t.Logf("on disk: blocks %d bytes for %d samples = %.2f bytes/sample (chunks %d, index %d); WAL now %d bytes",
		blocks, st.BlockSamples, float64(blocks)/float64(st.BlockSamples), st.ChunkBytes, st.IndexBytes, walBytes)
	if levels[2] == 0 {
		t.Fatal("no merged blocks")
	}
	if st.HeadMaxT-st.HeadMinT > 3*hour {
		t.Fatalf("head spans %d ms", st.HeadMaxT-st.HeadMinT)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	start = time.Now()
	db2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	t.Logf("reopened in %v: replayed %d WAL records after the checkpoint", time.Since(start).Round(time.Millisecond), db2.Replay.Records)
	verify(t, db2, n, hours)
}

// The same 24 hours with only 2-hour blocks, then merged: query time and
// disk size before and after.
func TestMergeEffect(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, NoSync: true, NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const n, hours = 1000, 24
	load(t, db, n, hours)

	measure := func() (int, time.Duration, time.Duration, int64) {
		one := eq("instance", "host-500")
		var tOne, tAll time.Duration
		const reps = 20
		for range reps {
			s := time.Now()
			if r := db.Select(0, hours*hour, one); len(r) != 1 {
				t.Fatal("one series")
			}
			tOne += time.Since(s)
		}
		s := time.Now()
		if r := db.Select(0, hours*hour, eq("job", "api")); len(r) != n {
			t.Fatal("all series")
		}
		tAll = time.Since(s)
		st := db.Stats()
		return len(st.Blocks), tOne / reps, tAll, st.IndexBytes
	}
	b1, one1, all1, idx1 := measure()
	start := time.Now()
	if err := db.MergeBlocks(); err != nil {
		t.Fatal(err)
	}
	mergeTook := time.Since(start)
	b2, one2, all2, idx2 := measure()
	t.Logf("before merge: %d blocks, 1 series over 24 h %v, all %d series %v, index %d bytes", b1, one1.Round(time.Microsecond), n, all1.Round(time.Millisecond), idx1)
	t.Logf("after merge (%v): %d blocks, 1 series %v, all series %v, index %d bytes", mergeTook.Round(time.Millisecond), b2, one2.Round(time.Microsecond), all2.Round(time.Millisecond), idx2)
	if b2 >= b1 || idx2 >= idx1 {
		t.Fatalf("merge didn't reduce blocks (%d->%d) or index (%d->%d)", b1, b2, idx1, idx2)
	}
	verify(t, db, n, hours)
}

func TestRetentionDeletesOldBlocks(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), NoSync: true, Retention: 6 * hour})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	load(t, db, 20, 24)
	st := db.Stats()
	oldest := st.Blocks[0].MinT
	// Ranges include both ends; the oldest kept block starts exactly at 12 h.
	if got := db.Select(0, 12*hour-1, eq("job", "api")); len(got) != 0 {
		t.Fatalf("data from 12 h ago is still there: %d series", len(got))
	}
	t.Logf("retention 6 h: %d blocks deleted; oldest kept starts at %.0f h", db.Deleted, float64(oldest)/float64(hour))
	if db.Deleted == 0 || oldest < 12*hour {
		t.Fatalf("deleted %d, oldest %d", db.Deleted, oldest)
	}
}
