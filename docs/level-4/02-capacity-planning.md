---
description: "Real measured per-record disk overhead and the real multiplying effect of replication factor on total cluster storage, from an actual 3-broker run."
---

# 02 · Capacity Planning: Partitions, Replication & Disk

"How much disk do I need" has a precise, measurable answer once you know two real numbers: the actual per-
record overhead Kafka's storage format adds on top of your payload, and the multiplying effect of replication
factor. This lesson measures both directly rather than estimating them.

## Worked example: real overhead, real replication multiplication

```text
$ bin/kafka-topics.sh --create --topic capacity-test --bootstrap-server localhost:9092 \
    --partitions 1 --replication-factor 3

$ bin/kafka-producer-perf-test.sh --topic capacity-test --num-records 100000 --record-size 200 \
    --producer-props bootstrap.servers=localhost:9092 acks=all
100000 records sent, 65789 records/sec (12.55 MB/sec), 402.19 ms avg latency, ...

$ # on-disk size, per replica, on each of the 3 brokers
node1/data/capacity-test-0: 20996211 bytes
node2/data/capacity-test-0: 20996211 bytes
node3/data/capacity-test-0: 20996211 bytes
```

(Real output from this course's 3-broker lab cluster.) The raw payload sent was exactly
`100,000 records × 200 bytes = 20,000,000 bytes`. Each replica's actual on-disk size was
**20,996,211 bytes** — identical across all three brokers (confirming perfect replication, Level 1 Lesson 7) —
meaning Kafka's storage format added **996,211 bytes of real overhead** for this run, or about **9.96 bytes
per record** (batch headers, offsets, timestamps, CRCs — the structural metadata Level 1 Lesson 3's log dump
showed surrounding every record). With replication factor 3, the **total** cluster-wide storage this one
topic actually consumes is `3 × 20,996,211 ≈ 63 MB` — roughly **three times** the raw payload size, not because
anything is being duplicated wastefully, but because that's exactly what "three independent, complete copies"
means on disk.

## How It Actually Works

Per-record overhead comes from the same batch/record structure Level 1, Lesson 3 dumped directly: each batch
carries header fields (producer ID, base offset, CRC, compression metadata) and each record within it carries
its own offset delta, timestamp delta, and length-prefixing — real, measurable bytes beyond your payload that
don't disappear no matter how the topic is configured. This overhead is roughly fixed per record (dominated by
per-record framing, with some further overhead amortized across a batch) — which is exactly why **larger
records, or better batching**, improve the storage-overhead percentage: the fixed per-record/per-batch cost
gets divided across more payload bytes, the same economy of scale that made batching help throughput in
Level 2.

Replication factor's effect on storage is intentionally simple to reason about precisely because it's linear
and exact: a replication factor of `N` means `N` complete, independent copies of every byte (payload plus
overhead) exist somewhere in the cluster, full stop — there's no deduplication or cleverness reducing that
multiplication, because the entire point of replication is redundant, independently-readable copies
(Level 1, Lesson 7), not a space-efficient encoding. This is precisely the number capacity planning has to
multiply against raw expected data volume: a system ingesting 1 TB/day at replication factor 3 needs roughly
3 TB/day of real disk write capacity across the cluster, before even accounting for retention period
multiplying that further (Level 1, Lesson 9 — a 7-day retention window on that same topic needs roughly
21 TB of steady-state disk, cluster-wide).

## Common mistakes

- **Sizing storage from raw payload volume alone, ignoring per-record overhead and replication factor.**
  As measured directly above, both are real, multiplicative factors — a plan based on payload size alone will
  under-provision by a predictable, measurable amount.
- **Assuming overhead percentage is constant regardless of record size.** Since overhead is largely
  per-record/per-batch (roughly fixed), it's a much larger *percentage* of very small records than of large
  ones — capacity planning for a high-volume, small-record workload (sensor readings, say) needs this overhead
  taken seriously; for large records (full documents, images) it's comparatively negligible.
- **Forgetting retention period is a direct multiplier on top of replication factor.** Total steady-state
  storage is approximately `daily ingest rate × replication factor × retention days` — leaving retention out
  of the capacity math is a common and expensive oversight.

## Exercise

Produce a known number of fixed-size records to a topic with a known replication factor, measure the real
on-disk size per replica with `du`/`ls`, and compute your own measured per-record overhead and total
replication multiplier — then use those two real numbers (not textbook estimates) to project disk needs for a
larger, hypothetical daily ingest volume and retention period.
