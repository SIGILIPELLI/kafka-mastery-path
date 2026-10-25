---
description: "Real producer benchmarks showing acks and batching's actual effect — including acks=all with good batching beating acks=0 with none."
---

# 02 · Producer Configuration That Actually Matters

"`acks=all` is slow, use `acks=0` or `acks=1` for performance" is common advice, and this lesson's own real
benchmark numbers show it's an oversimplification — the honest result is more interesting, and more useful,
than the folk wisdom.

## The configs that actually move the needle

**`acks`** controls how many replicas must confirm a write before the producer considers it successful: `0`
(fire and forget — don't wait for any acknowledgment at all), `1` (wait for the partition leader only), `all`
(wait for every replica currently in the ISR, Level 1 Lesson 7 — the only setting that can't lose
already-acknowledged data to a leader failure). **`linger.ms`** and **`batch.size`** control batching
(Level 1, Lesson 4) — how long to wait, and how much data to accumulate, before sending a batch rather than
sending immediately.

## Worked example: three real producer-perf-test runs

```text
$ bin/kafka-producer-perf-test.sh --topic perf-topic --num-records 100000 --record-size 100 --throughput -1 \
    --producer-props bootstrap.servers=localhost:9092 acks=0 linger.ms=0
100000 records sent, 303030 records/sec (28.90 MB/sec), 5.16 ms avg latency, 24 ms 95th

$ bin/kafka-producer-perf-test.sh ... acks=all linger.ms=0
100000 records sent, 186916 records/sec (17.83 MB/sec), 225.45 ms avg latency, 250 ms 95th

$ bin/kafka-producer-perf-test.sh ... acks=all linger.ms=20 batch.size=65536
100000 records sent, 330033 records/sec (31.47 MB/sec), 1.72 ms avg latency, 4 ms 95th
```

(Real benchmark numbers from this course's lab broker — a 3-partition topic, 100,000 records, 100 bytes each,
sent as fast as the client allows, on ordinary laptop hardware; your own numbers will differ, but the
*relationship* between the three runs is the real point.) `acks=0` with no batching gets a respectable 303K
records/sec. Switching to `acks=all` with the **same** no-batching settings drops throughput by nearly half
(186K records/sec) and inflates average latency more than 40x (5ms → 225ms) — this is the result that matches
the folk wisdom, and it's real. But the third run — `acks=all`, with `linger.ms=20` and a larger `batch.size`
— **beats even the first run**: 330K records/sec, and the *lowest* average latency of all three (1.72ms). The
honest conclusion isn't "acks=all is slow" — it's "sending records one at a time, un-batched, is slow, and
that cost dominates far more than the acks setting does once batching is actually working."

## How It Actually Works

Without batching, each record effectively pays its own full network round-trip cost, and with `acks=all`, that
round-trip includes waiting for the full ISR to replicate — which is why the no-batching `acks=all` run shows
such high latency: every single record, individually, pays that full replication wait. With batching enabled,
many records share **one** round-trip: the producer accumulates records client-side for up to `linger.ms`
milliseconds (or until `batch.size` bytes accumulate, whichever comes first), then sends the whole batch as one
request, and the broker replicates and acknowledges the *entire batch* as one unit. The ISR round-trip cost is
still there with `acks=all` — it's just now amortized across potentially thousands of records in one batch
instead of paid once per record, which is exactly why the batched `acks=all` run's *average* latency per record
came out lower than the un-batched `acks=0` run's: fewer total round-trips, even though each round-trip itself
does more work (a full ISR acknowledgment instead of none).

This also explains why `linger.ms=20` — adding a 20-millisecond delay before sending — produced a **faster**
result, not a slower one, despite intuition suggesting "waiting longer must be slower." The 20ms isn't wasted
time; it's exactly the window that lets many records combine into one efficient batch instead of many small,
round-trip-dominated ones. The real tradeoff linger.ms controls is: too small, and you under-batch and pay
excess round-trip overhead (as the second run shows); too large for your actual traffic volume, and you add
real, unnecessary delay for applications where each individual record's latency matters more than aggregate
throughput. There's no universally correct value — it has to be tuned against your own traffic pattern and
latency requirements, the same way this lesson measured it rather than assumed it.

## Common mistakes

- **Defaulting to `acks=0` or `acks=1` for "performance" without testing whether batching alone would have
  solved the actual problem.** As this lesson's numbers show, the durability you give up with weaker `acks`
  settings may not even be the performance lever you think it is once batching is configured well.
- **Setting `linger.ms` to a large value by copying a benchmark's "optimal" setting without validating it
  against your own traffic volume and latency budget.** A 20ms linger made sense for this lesson's
  high-volume synthetic benchmark; it would be a poor choice for a producer sending one record every few
  seconds, where that 20ms is pure added latency with no batching benefit to offset it.
- **Benchmarking producer settings against a single-partition, single-broker topic and assuming the numbers
  generalize.** Partition count, replication factor, and real network topology between brokers all affect
  these numbers meaningfully — treat any one benchmark (including this lesson's) as directional, not as a
  universal constant to copy into a different environment.

## Exercise

Run `kafka-producer-perf-test.sh` yourself against a real (even single-broker) cluster with at least three
different `acks`/`linger.ms`/`batch.size` combinations, and record the real throughput and latency numbers for
each — do not guess. Then identify, from your own results, whether this lesson's specific finding (batched
`acks=all` beating un-batched `acks=0`) holds on your hardware too, or whether your numbers tell a different
story — and if they differ, consider what about your environment (disk speed, network, CPU) might explain it.
