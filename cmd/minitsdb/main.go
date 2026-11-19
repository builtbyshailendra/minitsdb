// Command minitsdb runs the database.
//
//	minitsdb serve  -dir DIR -listen 127.0.0.1:9201   remote write in, PromQL API out
//	minitsdb ingest -dir DIR -from N                  write numbered batches and print "ack N"
//	                                                  after each is durable (used by the crash tests)
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/builtbyshailendra/minitsdb/labels"
	"github.com/builtbyshailendra/minitsdb/server"
	"github.com/builtbyshailendra/minitsdb/tsdb"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: minitsdb serve|ingest [flags]")
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	dir := fs.String("dir", "data", "data directory")
	segSize := fs.Int64("segment-size", 0, "WAL segment size in bytes (0 = 32 MiB)")
	blockRange := fs.Duration("block-range", 2*time.Hour, "time span of a level-1 block")
	retention := fs.Duration("retention", 0, "delete blocks older than this (0 = keep everything)")
	listen := fs.String("listen", "127.0.0.1:9201", "serve: address to listen on")
	compactInterval := fs.Duration("compact-interval", time.Minute, "serve: how often to run compaction")
	from := fs.Int("from", 0, "ingest: first batch number")
	batches := fs.Int("batches", 1<<30, "ingest: number of batches")
	series := fs.Int("series", 50, "ingest: series per batch")
	compactEvery := fs.Int("compact-every", 0, "ingest: run compaction every N batches (0 = never)")
	_ = fs.Parse(args)

	db, err := tsdb.Open(tsdb.Options{Dir: *dir, SegmentSize: *segSize, BlockRange: blockRange.Milliseconds(), Retention: retention.Milliseconds()})
	if err != nil {
		fail(err)
	}
	r := db.Replay
	fmt.Printf("replayed segments=%d records=%d torn=%d dropped=%d cleaned=%d\n", r.Segments, r.Records, r.TornBytes, r.Dropped, db.Cleaned)

	switch cmd {
	case "serve":
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
		defer stop()
		srv := &http.Server{Addr: *listen, Handler: server.New(db), ReadHeaderTimeout: 10 * time.Second}
		go func() {
			t := time.NewTicker(*compactInterval)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					if err := db.Compact(); err != nil {
						fmt.Fprintln(os.Stderr, "compact:", err)
					}
				}
			}
		}()
		go func() {
			<-ctx.Done()
			sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = srv.Shutdown(sctx)
		}()
		fmt.Printf("listening on %s\n", *listen)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fail(err)
		}
		if err := db.Close(); err != nil {
			fail(err)
		}
	case "ingest":
		for i := *from; i < *from+*batches; i++ {
			res, err := db.Append(Batch(i, *series))
			if err != nil {
				fail(err)
			}
			if res.Rejected > 0 {
				fail(fmt.Errorf("batch %d: %d rejected: %v", i, res.Rejected, res.FirstErr))
			}
			fmt.Printf("ack %d ignored=%d\n", i, res.Ignored)
			if *compactEvery > 0 && i%*compactEvery == 0 {
				if err := db.Compact(); err != nil {
					fail(err)
				}
			}
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
