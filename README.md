# minitsdb

A small time-series database in the style of Prometheus's storage: Gorilla-compressed chunks, a write-ahead log, an in-memory head block, immutable blocks on disk with an inverted index, compaction, a subset of PromQL, and a Prometheus remote-write receiver.

Built pairing with Claude Code, as a learning project. Go 1.27; runs on this laptop only. All data is synthetic.

**What it shows:**
- **Compression with numbers:** 0.40 to 7.60 bytes per sample depending on the series (1.72 on disk for counters, index included), and why: decimals and scrape jitter are the expensive cases.
- **Durability:** 60 runs killed at random and at 8 named points (mid-write, before fsync, inside compaction) lose no acknowledged sample and leave no partial batch or duplicate.
- **PromQL that matches Prometheus:** 361,719 points from 520 random range queries equal Prometheus's own engine; four deliberate engine bugs are each caught by that comparison.
- **Wire compatibility:** remote-write requests built with Prometheus's own types decode exactly, and a server killed mid-stream loses nothing.

Learning notes and explain-it questions: [LEARN.md](LEARN.md). Interview brief: [`../briefs/minitsdb.md`](../briefs/minitsdb.md).

```text
POST /api/v1/write (snappy + protobuf)          GET /api/v1/query, /query_range
        │                                                 ▲
        ▼                                                 │ promql: parse, fetch once, evaluate per step
   tsdb.Append ──► WAL (CRC-framed records, fsync) ──► ack
        │
        ▼
   head (memory): series → Gorilla chunks of 120 samples; postings label → series refs
        │ Compact(): oldest 2 h → block; WAL checkpoint; old segments deleted
        ▼
   blocks/ (immutable): chunks + index (symbols, series, postings) + meta.json
        │ 3 × 2 h → one 6 h block; retention deletes old blocks
        ▼
   queries read the overlapping blocks and the head, and merge by labels
```

## Build log

### Step 1: Gorilla compression

`chunk/` encodes up to 120 samples per chunk, as in Facebook's Gorilla paper (VLDB 2015) and Prometheus's XOR chunks:
- **Timestamps:** the first as a varint, then the first gap, then only the change in the gap (delta-of-delta) in a variable-width bucket. At a steady scrape interval the change is 0 and costs **1 bit**.
- **Values:** XOR with the previous value. Equal values cost **1 bit**; otherwise only the meaningful bits between the leading and trailing zeros are stored, reusing the previous window when it fits.

Tests: round trips bit for bit (NaN payloads including Prometheus's staleness marker, ±Inf, -0, every timestamp bucket and its edges); a truncated chunk is an error, never a panic; **2.34 million fuzz cases in 60 s** with no mismatch.

Bytes per sample, 200 chunks of each kind of synthetic series (`TestCompression`):

| Series (15 s scrape interval) | Bytes per sample | vs 16 raw bytes |
|---|---|---|
| constant (`up`, a flag) | 0.40 | 40x smaller |
| counter, +0..10 per scrape | 1.64 | 9.8x |
| counter, busy (+0..1000) | 2.56 | 6.2x |
| gauge, memory bytes (changes by whole pages) | 2.64 | 6.1x |
| counter with ±5 ms scrape jitter | 3.31 | 4.8x |
| gauge with 2 decimals (CPU %) | 7.11 | 2.3x |
| random float64 (worst case) | 7.60 | 2.1x |

For comparison, the Gorilla paper reports 1.37 bytes per sample on Facebook's production data.

What the table taught me:
- **Decimals don't compress.** 37.42 has no exact binary form, so the XOR of two nearby decimals has bits all over the mantissa: 7.11 bytes, almost as bad as random. Integers and whole multiples (bytes, counts) compress well.
- **Jitter costs as much as the value.** ±5 ms of scrape jitter doubled a counter's cost (1.64 → 3.31): each timestamp change now takes a 16-bit bucket instead of 1 bit.

Speed: appending 42.6 ns per sample, reading 22.9 ns per sample. The first version wrote one bit at a time (72.7 ns); writing up to a byte at once took it to 42.6.

### Step 2: the head, the write-ahead log, and crashes

- **Write path** (`tsdb.Append`): resolve or create each series, check the sample is newer than the series' last one, write one WAL record for new series and one for samples, **fsync**, and only then add the samples to the in-memory head and return. A nil error means durable.
- **Rules:** a sample older than its series' newest is rejected (out of order); the same timestamp with a different value is rejected (duplicate); the same timestamp *and* value is ignored, so a retried write is harmless.
- **WAL** (`wal/`): numbered segment files of records framed as type | length | CRC32C | payload. On open, every record is replayed into the head. A short or corrupt record ends the log: the segment is truncated there, and later segments are deleted. After a crash only the tail can be damaged, and nothing after the damage was acknowledged.
- **Head** (`tsdb/head.go`): per series a list of full chunks plus the open chunk; an inverted index from each `name=value` to sorted series refs, so a query intersects the lists for its equality matchers and filters by the rest (`!=`, `=~`, `!~`; regexes anchored like Prometheus).

Tests:
- append rules, matcher types, time ranges; a restart replays 501 records into identical series;
- half a record appended by hand is cut off on open (29 bytes) and writing continues after the good data;
- a flipped bit in the second of six segments: replay keeps 13 of 40 samples and drops the 3 later segments, and says so;
- **crash test** (`e2e`): 40 runs of a separate `minitsdb ingest` process on one data directory, each resuming after the last acknowledged batch of 50 samples and each killed: 13 times from outside at a random moment, 9 halfway through a WAL write, 10 after the write but before fsync, 8 after fsync but before the samples reached memory. Result: **all 5,023 acknowledged batches present, none partial.** 30 batches written but never acknowledged were found already stored when resent, so the retry was a no-op. Replay cut 2,970 torn bytes in total.

What fsync costs on this laptop (`TestAppendThroughput`):

| Writes | Throughput |
|---|---|
| 1 sample per write, fsync each | 376 samples/s (2.7 ms per write) |
| 100 per write, fsync each | 34,640 samples/s |
| 1,000 per write, fsync each | 221,067 samples/s |
| 1,000 per write, no fsync (unsafe) | 1,807,550 samples/s |

The fsync takes about 2.7 ms whatever the size, so durability is affordable only in batches. That's why Prometheus remote write sends samples in batches.

Limit: these crashes kill the process, not the machine. Data the process wrote before dying is still in the operating system's cache. A power cut could lose writes that weren't fsynced, which this laptop can't simulate; the design covers it (acknowledge only after fsync), but it is untested.

### Step 3: blocks on disk, compaction, retention

- **Blocks** (`tsdb/block.go`): immutable directories holding `[MinT, MaxT)`: a `chunks` file (chunk bytes back to back), an `index` (a symbol table, then each series' labels as symbol numbers plus where its chunks are, then postings lists of series numbers stored as gaps, then a CRC32C) and `meta.json`. Written to `<id>.tmp`, fsynced, then renamed: a crash never leaves a half block under a real name. Chunks are read on demand with `ReadAt`; the index is loaded into memory.
- **Cutting the head** (`Compact`): when the head spans more than 1.5 block ranges (2 h by default), its oldest aligned range becomes a level-1 block, those samples are dropped from memory (and series with nothing left are forgotten), and the WAL is **checkpointed**: the head's remaining content goes to `wal/checkpoint.N`, older segments are deleted. Replay reads the newest checkpoint, then segments from N on.
- **Crash between steps:** the head's lower bound is the newest block's end, so on replay WAL samples already in a block are skipped. Leftover `.tmp` block directories are deleted on open, and so is any block whose sources are all inside a bigger block (a merge that died before deleting its inputs).
- **Merging:** level-1 blocks in the same 3-range (6 h) window are merged into one level-2 block once the head has moved past it; chunks are copied as they are, not re-encoded.
- **Retention:** blocks that end more than the retention period before the newest data are deleted.
- **Queries** read the overlapping blocks and the head, and merge series by labels.

Results:

| Test | Result |
|---|---|
| 24 h × 200 series at 15 s (1,152,000 samples), compacting hourly | 755,000 samples/s without fsync; 3 level-2 and 2 level-1 blocks; head holds 2 h (96,000 samples); every sample reads back exactly, before and after a restart |
| Disk use | **1.72 bytes per sample** in blocks, index included (chunks 1.68 MB, index 133 KB for 1,056,000 samples) |
| Reopen after compaction | 9 ms (checkpoint plus 0 records after it) |
| Merge, 24 h × 1,000 series | 11 blocks → 5; index 851 KB → 676 KB (−21%); one series over 24 h 510 → 482 µs; all 1,000 series 467 → 422 ms; merge took 263 ms |
| Retention 6 h | 2 blocks deleted; nothing older than 12 h left |

Merging helped less than I expected here: every index is held in memory, so opening fewer blocks saves little. With indexes read from disk, as in Prometheus, fewer and larger blocks matter more.

**Crash test, extended:** 60 runs with 10-minute blocks and compaction every 20 batches, killed at 8 kinds of moment. Besides the 5 from step 2, 8 kills after a block's chunks were written (before the rename), 9 after a block was in place (before the head was trimmed), 6 after a checkpoint (before old segments were deleted), and 8 after a merge (before its sources were deleted). **All 6,721 acknowledged batches present, none partial, no timestamp twice;** restarts removed 32 half-written or already-merged block directories.

**What broke:** the retention test failed because it asked for data "up to 12 h" and got samples. Query ranges include their end, and the oldest kept block starts exactly at 12 h. The test was wrong; it now asks up to 12 h minus 1 ms.

### Step 4: a subset of PromQL, checked against Prometheus's own engine

`promql/` parses and evaluates: selectors with `=`, `!=`, `=~`, `!~`; range selectors (`x[5m]`); `rate`, `increase`, and `avg/sum/min/max/count_over_time`; `sum`, `avg`, `min`, `max`, `count` with `by (...)` or `without (...)` in either position; and `+ - * /` between numbers, between a vector and a number, and between vectors whose labels match one to one. Instant and range queries; each selector's data is fetched once per query and every step is evaluated in memory.

Semantics follow Prometheus 3: an instant selector takes the newest sample in `(t − 5m, t]`, a range selector the samples in `(t − range, t]`. `rate` adds back the value before every counter reset, extrapolates towards each edge of the window only if the gap is under 1.1× the average sample spacing (otherwise by half a spacing), and never extrapolates a counter below zero. A worked example by hand is in `TestRateByHand`.

**The oracle** (`TestMatchesPrometheus`): the same samples go into minitsdb and into Prometheus's own storage (`util/teststorage`, Prometheus v0.316.0, imported by the test only). 26 query shapes run over random windows with odd step sizes, through both engines, and every point is compared (relative tolerance 1e-9). The data is deliberately awkward: scrape jitter, 5% missed scrapes, counter resets, series that start late or stop early, float gauges.

**Result: 520 range queries, 361,719 points, 0 mismatches.**

**Proof the oracle has teeth** (`scripts/mutate.sh` breaks the engine one way at a time):

| Deliberate mistake | Mismatches found |
|---|---|
| range window includes its left edge (Prometheus 2 behaviour) | 15 |
| counter resets ignored | 47,503 |
| extrapolation threshold 1.0× instead of 1.1× | 4,541 |
| no zero-point limit | 3,554 |

**What broke:** the first version of this test passed with 0 mismatches on the first try, and so did all four mutations. A test that can't fail is worthless, so I printed one query from both engines: both were empty. My data generator compared a relative timestamp with an absolute one, so every counter series kept only its first sample, and the two engines agreed on nothing. After fixing it the test compares 361,719 points instead of 49,702. It also now fails if any query shape yields fewer than 200 points.

### Step 5: remote write in, the Prometheus query API out

- **`POST /api/v1/write`** (`remote/`, `server/`): Prometheus remote write 1.0, a snappy-compressed protobuf `WriteRequest`. The protobuf is decoded by hand with `protowire` (four small messages; unknown fields such as metadata and exemplars are skipped; native histograms are counted and ignored). Status codes are what Prometheus's sender expects: 204 stored; 400 for requests that can never succeed (bad encoding, out-of-order or too-old samples), which the sender drops; 500 when a retry might help.
- **`/api/v1/query` and `/api/v1/query_range`**: the PromQL subset, answering in Prometheus's JSON format (Unix-second timestamps, values as strings), so Grafana could point at it (not tried).
- **`/metrics`**: the server's own counters in the Prometheus text format; **`/-/healthy`**.
- `minitsdb serve` runs compaction on a timer.

Results:

| Test | Result |
|---|---|
| Requests built with Prometheus's own `prompb` types (with exemplars, metadata, a native histogram, empty label values, a negative timestamp, -Inf) | decoded exactly |
| Fuzzing the decoder | 892,900 inputs in 30 s, no panic |
| 2 h of node-exporter-like scrapes (20 hosts × 6 series × 480 scrapes = 57,600 samples) sent like Prometheus, **server killed at scrape 200** and restarted | sender retried 5 times, nothing lost: 480 points for every `up` series; all 440 points of `sum by (mode) (rate(cpu_seconds_total[5m]))` equal the exact rate; gauges exact in the 3 blocks and the head |
| A sample older than the head | 400: "sample older than the head (already in a block)" |
| Throughput: 100 requests of 2,000 samples (Prometheus's default batch), one client | 158,237 samples/s, 12.6 ms per request including the fsync |

**Found while testing:** a -0 sent with Prometheus's encoder arrives as +0. proto3 doesn't write a field equal to its zero value, and -0 == 0, so the value is left out and the receiver sees the default. Remote write 1.0 can't carry the sign of zero.

To point a real Prometheus at it (not tried: there's no Prometheus on this laptop):

```yaml
remote_write:
  - url: http://127.0.0.1:9201/api/v1/write
```

## Run it

```bash
go test ./...                                   # under a minute
go run ./cmd/minitsdb serve -dir data           # remote write on :9201
curl 'http://127.0.0.1:9201/api/v1/query?query=count(up)'
bash scripts/mutate.sh                          # four engine bugs, each caught by the Prometheus comparison
```

## What it doesn't do

- **No native histograms, exemplars, metadata, staleness markers or out-of-order ingestion.** Out-of-order samples are rejected, as Prometheus does by default.
- **A small PromQL:** no `offset`, `@`, subqueries, `histogram_quantile`, comparison or set operators, `on`/`ignoring`/`group_left` matching, or `irate`/`delta`/`deriv`.
- **Crashes are process kills.** Power loss (unflushed writes lost by the OS) isn't simulated; the design acknowledges only after fsync, but that path is untested.
- **Block indexes are loaded fully into memory;** Prometheus memory-maps them. Chunks are read with `ReadAt`, not memory-mapped.
- **One writer at a time** (a database-wide lock), and compaction blocks writes while it cuts a block.
- **Not tested with a real Prometheus or Grafana:** compatibility was checked with Prometheus's own types and engine, not its running binary.
- No CI, not published.
