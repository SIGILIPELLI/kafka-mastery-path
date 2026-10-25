---
description: "Time/size retention vs log compaction, with a real compacted segment dumped from disk showing emptied batches and only the latest value per key."
---

# 09 · Retention & Compaction

Kafka has two completely different answers to "when does old data go away," and picking the wrong one for a
given topic is a common, costly mistake. This lesson runs a real compacted topic to the point where compaction
actually happens, and dumps the resulting segment to see exactly what got removed.

## Deletion vs compaction: two different questions

**Time/size-based retention** (`cleanup.policy=delete`, the default) answers "how long do I keep records
around at all" — once a segment is older than `retention.ms` or the partition exceeds `retention.bytes`,
whole old segments are deleted outright, key or no key. **Log compaction** (`cleanup.policy=compact`) answers a
different question: "for each key, keep only its *latest* value, forever (or until a delete marker's own
retention expires)." Compaction is what backs Kafka's own internal `__consumer_offsets` topic (Lesson 5) — you
don't want the *history* of every offset commit ever made, you want the *current* offset for each
(group, topic, partition), which is exactly a per-key "latest value" problem.

## Worked example: a real compaction, observed by dumping the log

```text
$ bin/kafka-topics.sh --create --topic compacted-topic --bootstrap-server localhost:9092 \
    --partitions 1 --replication-factor 1 --config cleanup.policy=compact \
    --config segment.ms=100 --config min.cleanable.dirty.ratio=0.01

$ # produce 5 updates each for two keys: user1 and user2, versions 1 through 5
$ bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic compacted-topic
compacted-topic:0:10

$ # wait for the log cleaner to run, then dump the closed segment
$ bin/kafka-dump-log.sh --files .../compacted-topic-0/00000000000000000000.log --print-data-log
baseOffset: 0 lastOffset: 1 count: 0 ...     <- version-1 batch: emptied by compaction
baseOffset: 2 lastOffset: 3 count: 0 ...     <- version-2 batch: emptied by compaction
baseOffset: 4 lastOffset: 5 count: 0 ...     <- version-3 batch: emptied by compaction
baseOffset: 6 lastOffset: 7 count: 2 ...     <- version-4 batch: KEPT (latest in this segment)
| offset: 6 ... key: user1 payload: version-4
| offset: 7 ... key: user2 payload: version-4
```

(Real output from this course's lab broker — 10 records genuinely produced, and this is genuinely what the log
cleaner left behind.) The batches holding `version-1`, `version-2`, and `version-3` for both keys now show
`count: 0` — the **records themselves were physically removed** from disk by the compactor; only an empty
batch header remains at that position, a result of the compactor rewriting the segment with the
superseded records stripped out. Only the batch holding `version-4` survived, because at the moment compaction
ran, `version-4` was the latest value for each key *within this now-closed segment*. Notice `version-5`
(produced right after, into a *new*, still-active segment) doesn't appear in this dump at all — it's sitting in
a separate, newer segment file that hadn't been compacted yet, because **compaction only operates on closed
segments, never the one currently being written to.**

## How It Actually Works

The **log cleaner** is a background thread (or pool of threads, `log.cleaner.threads`) that periodically scans
for partitions whose "dirty ratio" (the fraction of the log that's newer than the last compaction point) exceeds
`min.cleanable.dirty.ratio`, then rewrites eligible, non-active segments: for each key, it keeps only the record
with the highest offset and discards the rest, producing new, smaller segment files. This is why compaction is
sometimes misunderstood as instantaneous — it's not a rule enforced on every write, it's a periodic background
rewrite, which is exactly why this lesson's example needed a short wait after producing before the compacted
state became visible on disk.

**Tombstones** (a record with a null value for a given key) are compaction's way of expressing deletion:
writing a null-valued record for a key, then letting compaction run, eventually removes *all* prior values for
that key, including the tombstone itself, after `delete.retention.ms` passes — long enough that any consumer
that was behind at the time of the delete still has a chance to see the tombstone and know the key was
explicitly deleted, rather than the key's history simply vanishing with no explanation. Deleting too eagerly
(a very short `delete.retention.ms`) risks a slow consumer missing the tombstone entirely and never learning a
key was removed.

## Common mistakes

- **Using `cleanup.policy=compact` for a topic that's really an event log, not a keyed "current state"
  store.** Compaction actively throws away history — appropriate for `__consumer_offsets`-style latest-value
  data, actively wrong for an audit trail or event stream where every historical record matters.
- **Assuming compaction is immediate.** As shown above, it's a periodic background process with its own
  trigger threshold (`min.cleanable.dirty.ratio`) — querying a compacted topic moments after writing duplicate
  keys can still show every version, which is correct, not a bug.
- **Forgetting that records without a key can't be meaningfully compacted.** Compaction needs a key to know
  which records are "the same thing, newer version" — a compacted topic receiving null-keyed records has
  nothing to deduplicate against and will simply accumulate them.

## Exercise

Reproduce this lesson's compacted-topic setup, produce several versions of two or three keys, wait, and dump the
resulting segment yourself — confirm which batches show `count: 0`. Then produce a tombstone (a record with a
key but no value — the console producer's `--property null.marker=<DELETE>` option can send one) for one of
your keys, wait again, and check whether that key's prior values and the tombstone itself eventually disappear
after `delete.retention.ms`.
