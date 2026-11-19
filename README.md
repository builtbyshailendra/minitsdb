# minitsdb

A small time-series database in the style of Prometheus's storage: Gorilla-compressed chunks, a write-ahead log, an in-memory head block, immutable blocks on disk with an inverted index, compaction, a subset of PromQL, and a Prometheus remote-write receiver.

Built pairing with Claude Code, as a learning project. Go 1.27; runs on this laptop only. All data is synthetic.

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
