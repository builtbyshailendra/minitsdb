// Command minitsdb runs the database.
//
//	minitsdb ingest -dir DIR -from N    write numbered batches and print "ack N"
//	                                    after each is durable (used by the crash tests)
package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/builtbyshailendra/minitsdb/labels"
	"github.com/builtbyshailendra/minitsdb/tsdb"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: minitsdb ingest [flags]")
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	dir := fs.String("dir", "data", "data directory")
	from := fs.Int("from", 0, "ingest: first batch number")
	batches := fs.Int("batches", 1<<30, "ingest: number of batches")
	series := fs.Int("series", 50, "ingest: series per batch")
	segSize := fs.Int64("segment-size", 0, "WAL segment size in bytes (0 = 32 MiB)")
	_ = fs.Parse(args)

	switch cmd {
	case "ingest":
		db, err := tsdb.Open(tsdb.Options{Dir: *dir, SegmentSize: *segSize})
		if err != nil {
			fail(err)
		}
		r := db.Replay
		fmt.Printf("replayed segments=%d records=%d torn=%d dropped=%d\n", r.Segments, r.Records, r.TornBytes, r.Dropped)
		for i := *from; i < *from+*batches; i++ {
			res, err := db.Append(Batch(i, *series))
			if err != nil {
				fail(err)
			}
			if res.Rejected > 0 {
				fail(fmt.Errorf("batch %d: %d rejected: %v", i, res.Rejected, res.FirstErr))
			}
			fmt.Printf("ack %d ignored=%d\n", i, res.Ignored)
		}
		_ = db.Close()
	default:
		fail(fmt.Errorf("unknown command %q", cmd))
	}
}

// Batch is batch i of the ingest workload: one sample per series at
// i * 15 s, value i * (series+1), so any sample can be checked.
func Batch(i, series int) []tsdb.Sample {
	b := make([]tsdb.Sample, series)
	for s := range series {
		b[s] = tsdb.Sample{
			Labels: labels.FromStrings(labels.MetricName, "ingest_test_total", "series", strconv.Itoa(s), "job", "crash"),
			T:      int64(i) * 15000,
			V:      float64(i * (s + 1)),
		}
	}
	return b
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "minitsdb:", err)
	os.Exit(1)
}
