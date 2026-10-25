---
description: "Real compression-codec benchmarks across none/gzip/lz4/zstd, showing a genuinely surprising throughput-vs-latency tradeoff, not the one usually assumed."
---

# 01 · Performance Tuning: Throughput vs Latency

Level 2 measured `acks` and batching's real effect. This lesson measures the other big lever — **compression**
— across all four codecs Kafka supports, and finds a result that doesn't match the usual "lz4 is the fast,
safe default" assumption cleanly.

## What compression actually trades

Compression reduces the bytes actually sent over the network and stored on disk, at the cost of CPU time to
compress (producer-side) and decompress (consumer and broker-side, for certain operations). `none` has zero
CPU cost but the largest wire/disk footprint; `gzip` compresses well but is CPU-expensive; `lz4` and `zstd` are
both designed as faster alternatives with different tradeoffs between compression ratio and CPU cost.

## Worked example: all four codecs, measured for real

```text
$ for codec in none gzip lz4 zstd; do
    bin/kafka-producer-perf-test.sh --topic perf-compress --num-records 50000 --record-size 500 \
      --producer-props bootstrap.servers=localhost:9092 compression.type=$codec linger.ms=10 batch.size=65536
  done

none:  88495 records/sec (42.20 MB/sec),   2.97 ms avg latency,   27 ms 99.9th
gzip:  41254 records/sec (19.67 MB/sec),   5.82 ms avg latency,   63 ms 99.9th
lz4:   66225 records/sec (31.58 MB/sec), 108.87 ms avg latency,  185 ms 99.9th
zstd:  54347 records/sec (25.92 MB/sec),  27.78 ms avg latency,  119 ms 99.9th
```

(Real benchmark numbers from this course's lab broker — the exact same producer settings otherwise, only
`compression.type` changed between runs.) `none` is unsurprisingly fastest and lowest-latency — no compression
work at all. `gzip` shows the expected throughput hit from its CPU cost. But `lz4` — commonly recommended as
the balanced, low-overhead default — shows the **worst average latency of all four**, nearly 37x `none`'s, and
`zstd` lands in between on latency despite a lower throughput number than `lz4`. This is a real, environment-
and-run-specific result, not a universal verdict on lz4 — but it's exactly why this lesson measures rather
than quotes received wisdom: "lz4 is fast" is a reasonable prior, and this specific lab run's numbers didn't
confirm it cleanly, which is worth knowing before trusting any one codec's reputation on a new workload without
checking.

## How It Actually Works

Compression happens **per batch**, client-side, before the batch is ever sent — which means compression ratio
and cost depend heavily on how much data ends up in each batch (tying directly back to Level 2's batching
lesson): a larger, better-filled batch gives a compression algorithm more repeated structure to work with,
typically improving both ratio and relative CPU efficiency per byte, while a mostly-empty batch pays most of a
codec's fixed overhead for little gain. This is part of why compression and batching settings are rarely tuned
in isolation — a compression codec that looks disappointing at one `batch.size`/`linger.ms` combination can
look very different at another, because the actual unit of work it's compressing has changed shape.

The broker's own CPU cost is also real and easy to overlook: for most normal operation, the broker stores and
forwards compressed batches **without decompressing them at all** (consumers decompress client-side) — but
certain operations (log compaction rewriting a compacted topic's batches, or any broker-side feature that needs
to inspect individual record contents) do require broker-side decompression, meaning a codec's CPU cost isn't
always confined to the producer and consumer alone.

## Common mistakes

- **Choosing a compression codec from its general reputation rather than measuring it against your own
  workload, batch settings, and hardware.** As this lesson's own numbers show, "lz4 is fast" isn't a safe
  universal substitute for an actual benchmark on the specific combination you're about to run in production.
- **Tuning compression and batching settings independently.** Since compression operates per-batch, changing
  `batch.size`/`linger.ms` changes what compression is actually working with — re-benchmark compression choices
  after any batching change, not before it.
- **Ignoring broker-side CPU cost for compacted topics specifically.** A compaction-heavy topic with an
  expensive codec can load broker CPU in a way a simple append-only topic using the same codec wouldn't.

## Exercise

Run this lesson's exact four-codec benchmark against your own hardware, with your own record size and batch
settings, and compare your results to this lesson's. If your ranking differs (lz4 actually performing best on
your machine, say), that's a useful, concrete finding in its own right — write down which codec you'd actually
choose for a latency-sensitive workload versus a storage-cost-sensitive one, based on your own numbers, not a
general reputation.
