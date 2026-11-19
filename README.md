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
