# Learning notes: minitsdb

## 1. What a time-series database is optimised for

Metrics are many series (one per label set, like `cpu_seconds_total{instance="host-07", mode="user"}`), each getting a new sample every scrape interval, appended in time order and almost never changed. Queries read recent ranges of many series and aggregate them. So the design favours:
- **cheap appends** (append to an open chunk, log, done),
- **compression** (samples are 16 bytes raw; most of that is predictable),
- **finding series by label** fast (an inverted index),
- **immutable, time-partitioned files** (old data is never rewritten, only merged or deleted whole).

Prometheus's storage (TSDB) is the model here: head block in memory + WAL, then 2-hour blocks on disk, compacted into larger ones.

## 2. Gorilla compression

From Facebook's Gorilla paper (VLDB 2015), used by Prometheus:
- **Timestamps:** store the first one, then the first gap, then only the *change* of the gap (delta-of-delta). A steady 15 s interval gives 0 every time: 1 bit. Jitter turns that into a 16-bit bucket.
- **Values:** XOR with the previous value. Equal: 1 bit. Otherwise store only the meaningful bits between the leading and trailing zeros; reuse the previous window if the bits fit.

Measured here: 0.40 bytes/sample for a constant, 1.64 for a slow counter, 2.64 for memory in whole pages, 3.31 for a counter with ±5 ms jitter, 7.11 for a gauge with two decimals, 7.60 for random floats. Two lessons:
- **Decimals don't compress.** 37.42 has no exact binary form; nearby decimals XOR to bits all over the mantissa.
- **Jitter is as expensive as the value.** That's why scrape timestamps should be regular.

## 3. Write path and the WAL

1. Check each sample (newer than the series' last one; the exact same sample again is ignored as a retry).
2. Append one WAL record for new series and one for samples: `type | length | CRC32C | payload`.
3. **fsync.** Only now is the write durable.
4. Apply to the in-memory head, then acknowledge.

On restart, replay the WAL. A crash can only damage the tail: a short or bad-checksum record is cut off, and anything after it is deleted (it can't have been acknowledged). fsync costs about 2.7 ms on this laptop whatever the size, so durability is affordable only in batches: 376 samples/s one at a time, 221,000/s in batches of 1,000.

## 4. Head, index and blocks

- **Head:** per series a list of full chunks (120 samples) plus the open one.
- **Postings (inverted index):** for every `name=value`, the sorted list of series that have it. A query intersects the lists of its equality matchers, then checks the rest (`!=`, regexes) on the survivors.
- **Blocks:** when the head spans 1.5 × 2 h, its oldest 2 h become an immutable directory (`chunks`, `index`, `meta.json`), written as `.tmp` and renamed so a crash never leaves a half block. Then the WAL is **checkpointed** (the head's remaining content goes to `checkpoint.N`; older segments are deleted).
- **Crash between steps:** the head's lower bound is the newest block's end, so replay skips WAL samples already in a block. `.tmp` directories and blocks already merged into a bigger one are deleted on open.
- **Compaction:** three 2-hour blocks become one 6-hour block (chunks copied, index rebuilt). Fewer blocks mean fewer indexes to search; here −21% index size, a small query speed-up, because the indexes are in memory anyway.
- **Retention:** delete whole blocks past the age limit. No rewriting needed.

## 5. PromQL semantics that matter

- **Instant selector at t:** the newest sample in `(t − 5m, t]` (the lookback). A series that stopped being scraped disappears after 5 minutes (Prometheus also writes staleness markers to end it sooner; minitsdb doesn't).
- **Range selector `x[5m]` at t:** the samples in `(t − 5m, t]`. Prometheus 3 made the left edge open; including it is wrong (caught 15 times in the comparison).
- **`rate`:** (last − first + every value before a reset) extrapolated to the window's edges, but only if the nearest sample is within 1.1 average gaps of the edge (else half a gap), and never below zero for a counter. That's why `rate` returns nothing when its window holds fewer than two samples (use a window of at least four scrape intervals), and why the rate of a perfectly linear counter comes out exact.
- **Aggregation:** `sum by (job)` keeps only `job`; `without (instance)` drops `instance` and the metric name.
- **Functions and arithmetic drop the metric name;** a plain selector keeps it.

## 6. Testing against an oracle

The best specification of PromQL is Prometheus's engine itself. The test loads the same samples into both systems and compares every point of 520 random range queries: 361,719 points, 0 mismatches. Two cautions from building it:
- **A test that can't fail proves nothing.** The first version passed, and so did four deliberate bugs, because a generator bug left every counter with one sample: the engines agreed on nothing. Now the test fails if any query shape produces fewer than 200 points, and `scripts/mutate.sh` proves four bugs are caught.
- **Make the data awkward:** jitter, missed scrapes, resets, series starting and stopping mid-window. That's where the extrapolation rules differ.

## 7. Remote write

Prometheus pushes samples as snappy-compressed protobuf `WriteRequest`s. The receiver answers 204 when stored, 400 when the request can never succeed (the sender drops it), 5xx when a retry might help (the sender retries with backoff). Decoding by hand with `protowire` shows how simple the format is: tag, wire type, value; unknown fields skipped. One surprise: proto3 omits fields equal to zero, so a -0 arrives as +0.

## 8. What broke while building

1. Writing one bit at a time made appends 72.7 ns/sample; writing up to a byte at once, 42.6.
2. My retention test asked for data "up to 12 h" and got samples: ranges include their end, and the oldest block starts at 12 h. The test was wrong.
3. The oracle test passed on nearly empty data (generator bug); fixed, plus a coverage guard and a mutation script.
4. A test expected -0 to survive remote write; proto3 drops it.

## 9. Explain it without notes

1. How does delta-of-delta encoding make a regular timestamp cost one bit?
2. How does XOR encoding store a value, and why do decimals compress badly?
3. Walk through an append: what happens before the write is acknowledged, and why?
4. What happens on restart if the last WAL record is half written? If a middle record is corrupt?
5. What is a postings list, and how does a query with `job="api", code!="500"` use them?
6. What is in a block, and why is it written to a `.tmp` directory first?
7. What is a WAL checkpoint, and what would go wrong without the "minimum valid time" check on replay?
8. Why merge blocks? Why didn't it speed queries up much here?
9. What does `rate(x[5m])` compute exactly? What happens at a counter reset?
10. Why is the range selector's left edge open, and how did the tests show it?
11. How do you know your PromQL matches Prometheus, and how do you know that check works?
12. What should a remote-write receiver return for an out-of-order sample, and why?

## 10. Your turn

1. **Out-of-order ingestion:** accept samples up to 10 minutes late into a separate out-of-order head, merged at query time and compaction.
2. **Memory-map the index** instead of loading it, and measure memory and query time on 100,000 series.
3. **`irate` and `histogram_quantile`:** add them, and add them to the oracle test.
4. **Run a real Prometheus** with `remote_write` pointing here, and Grafana on the query API.
5. **Power-loss testing:** wrap file writes in a layer that can "lose" everything after the last fsync, and rerun the crash test against it.
