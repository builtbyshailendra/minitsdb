// Package e2e runs minitsdb as a separate process and kills it.
package e2e

import (
	"bufio"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/builtbyshailendra/minitsdb/labels"
	"github.com/builtbyshailendra/minitsdb/tsdb"
)

var binary string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "minitsdb-bin")
	if err != nil {
		panic(err)
	}
	binary = filepath.Join(dir, "minitsdb.exe")
	if out, err := exec.Command("go", "build", "-o", binary, "../cmd/minitsdb").CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build: %v\n%s", err, out)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

const series = 50

// TestAcknowledgedSamplesSurviveCrashes runs the ingest process over and
// over on one data directory, killing it each time: either at a random
// moment from outside, or at a named point inside (halfway through a WAL
// write, after the write but before fsync, after fsync but before the
// samples reach memory). Each run resumes after the last acknowledged
// batch. At the end every acknowledged batch must be there, and every
// batch must be all there or not at all.
func TestAcknowledgedSamplesSurviveCrashes(t *testing.T) {
	dir := t.TempDir()
	rng := rand.New(rand.NewPCG(1, 2))
	points := []string{"", "", "wal.torn", "wal.before-sync", "head.after-wal",
		"block.after-chunks", "compact.after-block", "compact.after-checkpoint", "compact.after-merge"}
	compactionPoint := map[string]bool{"block.after-chunks": true, "compact.after-block": true, "compact.after-checkpoint": true, "compact.after-merge": true}
	lastAcked := -1
	var torn, dropped int64
	cleaned := 0
	resentFound := 0
	kills := map[string]int{}
	for round := range 60 {
		point := points[rng.IntN(len(points))]
		// 10-minute blocks (40 batches) and compaction every 20 batches, so
		// blocks are cut, merged and checkpointed many times per run.
		cmd := exec.Command(binary, "ingest", "-dir", dir, "-from", strconv.Itoa(lastAcked+1), "-series", strconv.Itoa(series),
			"-segment-size", "65536", "-block-range", "10m", "-compact-every", "20")
		cmd.Env = os.Environ()
		hit := 1 + rng.IntN(200)
		if compactionPoint[point] {
			hit = 1 + rng.IntN(3) // compactions are rarer than writes
		}
		if point != "" {
			cmd.Env = append(cmd.Env, fmt.Sprintf("MINITSDB_CRASH=%s:%d", point, hit))
		}
		out, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		killAfter := 1 + rng.IntN(300) // acks, for the outside kill
		sc := bufio.NewScanner(out)
		acks := 0
		for sc.Scan() {
			line := sc.Text()
			if strings.HasPrefix(line, "replayed") {
				var segs, recs int
				var tb, dr int64
				var cl int
				fmt.Sscanf(line, "replayed segments=%d records=%d torn=%d dropped=%d cleaned=%d", &segs, &recs, &tb, &dr, &cl)
				torn += tb
				dropped += dr
				cleaned += cl
				continue
			}
			var n, ignored int
			if _, err := fmt.Sscanf(line, "ack %d ignored=%d", &n, &ignored); err != nil {
				t.Fatalf("round %d: %q", round, line)
			}
			if ignored == series {
				resentFound++ // written before the last crash, never acked, already here
			}
			if n != lastAcked+1 {
				t.Fatalf("round %d: ack %d after %d", round, n, lastAcked)
			}
			lastAcked = n
			acks++
			if point == "" && acks == killAfter {
				_ = cmd.Process.Kill()
			}
		}
		err = cmd.Wait()
		how := point
		if how == "" {
			how = "killed from outside"
		}
		kills[how]++
		if point != "" {
			if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 137 {
				t.Fatalf("round %d: %s: %v", round, point, err)
			}
		}
	}

	db, err := tsdb.Open(tsdb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	torn += db.Replay.TornBytes
	got := db.Select(math.MinInt64, math.MaxInt64, labels.MustMatcher(labels.Equal, "__name__", "ingest_test_total"))
	if len(got) != series {
		t.Fatalf("%d series", len(got))
	}
	perBatch := map[int64]int{}
	for _, s := range got {
		k, _ := strconv.Atoi(s.Labels.Get("series"))
		for j, p := range s.Points {
			// A sample in a block and again in the head would show up twice.
			if j > 0 && p.T <= s.Points[j-1].T {
				t.Fatalf("series %d: timestamp %d repeated or out of order", k, p.T)
			}
			i := p.T / 15000
			if p.V != float64(i*int64(k+1)) {
				t.Fatalf("series %d batch %d: value %v", k, i, p.V)
			}
			perBatch[i]++
		}
	}
	for i := int64(0); i <= int64(lastAcked); i++ {
		if perBatch[i] != series {
			t.Fatalf("acknowledged batch %d has %d of %d samples", i, perBatch[i], series)
		}
	}
	extra := 0
	for i, n := range perBatch {
		if n != series {
			t.Fatalf("batch %d is partial: %d of %d samples", i, n, series)
		}
		if i > int64(lastAcked) {
			extra++
		}
	}
	st := db.Stats()
	t.Logf("60 runs (%v): %d batches acknowledged, all present, none partial, no timestamp twice. %d batches written but never acked were found already stored when resent (the retry was a no-op); %d more are past the last ack. Replay cut %d torn bytes. Restarts removed %d half-written or already-merged block dirs. Now %d blocks + head.",
		kills, lastAcked+1, resentFound, extra, torn, cleaned+db.Cleaned, len(st.Blocks))
}
