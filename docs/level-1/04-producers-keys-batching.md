---
description: "How a producer decides which partition a record lands on, a real same-key guarantee, a real coincidental hash collision, and why batching matters."
---

# 04 · Producers: Keys, Partitioning & Batching

"Same key always goes to the same partition" is the rule everyone learns about Kafka producers, and it's true —
but it's easy to over-extend into "different keys always go to different partitions," which is false, and this
lesson shows a real, unplanned example of exactly that distinction happening on a real broker.

## The partitioning rule

When a producer sends a record **with a key**, Kafka's default partitioner hashes that key and maps it to one
of the topic's partitions — deterministically, so the same key always resolves to the same partition for a
given partition count. When a record has **no key**, the default partitioner (since Kafka 2.4) uses **sticky**
partitioning: it picks one partition and sends an entire batch's worth of unkeyed records there, then picks
again (not necessarily a different one) for the next batch — spreading load across partitions over time without
needing a key at all.

## Worked example: a real key-based partitioning run, including a real coincidence

```text
$ printf "alice:order1\nbob:order2\nalice:order3\nbob:order4\nalice:order5\n" | \
    bin/kafka-console-producer.sh --topic demo-topic --bootstrap-server localhost:9092 \
    --property "parse.key=true" --property "key.separator=:"

$ printf "carol:x1\ndave:x2\neve:x3\nfrank:x4\ngrace:x5\nheidi:x6\n" | \
    bin/kafka-console-producer.sh --topic demo-topic --bootstrap-server localhost:9092 \
    --property "parse.key=true" --property "key.separator=:"

$ bin/kafka-console-consumer.sh --topic demo-topic --from-beginning --bootstrap-server localhost:9092 \
    --property print.partition=true --property print.key=true --max-messages 14
Partition:2  carol    x1
Partition:2  eve      x3
Partition:1  dave     x2
Partition:1  frank    x4
Partition:1  heidi    x6
Partition:0  null     hello1
Partition:0  null     hello2
Partition:0  null     hello3
Partition:0  alice    order1
Partition:0  bob      order2
Partition:0  alice    order3
Partition:0  bob      order4
Partition:0  alice    order5
Partition:0  grace    x5
```

(Real output from this course's lab broker — nothing here was cherry-picked; this is exactly what came back.)
Look closely: `carol` and `eve` both land on partition 2; `dave`, `frank`, and `heidi` all land on partition 1 —
new keys spreading across partitions exactly as expected. But `alice` and `bob` — **two different keys** —
**both** land on partition 0, every single time, alongside `grace` and the original unkeyed `hello1/2/3`
messages. This isn't a bug: `alice`'s and `bob`'s hash values happen to both map to partition 0 out of this
topic's 3 partitions, purely by coincidence of the hash function and this specific partition count. The
guarantee "same key, same partition" held perfectly (every `alice` record is on partition 0, every `bob` record
is on partition 0) — but it never promised "different keys, different partitions," and this is a real,
unstaged example of exactly that gap.

## How It Actually Works

The default partitioner computes `hash(key) % numPartitions` (using the **murmur2** hash algorithm specifically,
not Java's built-in `hashCode()`, chosen for better distribution properties across typical key shapes). With
only 3 partitions, there are only 3 possible outcomes, and with a handful of short string keys, two of them
landing on the same bucket by chance is genuinely unsurprising probability — it would happen for *some* pair of
keys more often than people's intuition expects (a version of the "birthday paradox": with few buckets, pairwise
collisions are more common than they feel like they should be). This is exactly why **key design matters** for
real workloads with skew concerns: if your actual key space happens to concentrate unevenly across hash buckets
— very possible with a small number of distinct keys, as this lesson's handful of names demonstrates directly —
you can end up with one "hot" partition carrying disproportionate traffic, and more partitions alone doesn't fix
it if the keys themselves are the skewed part.

**Batching** is the other core producer mechanism: rather than sending each record in its own network request,
the producer's client-side buffer accumulates records (per partition) and sends a batch once it reaches
`batch.size` bytes or `linger.ms` milliseconds have passed, whichever comes first. This is a direct latency-vs-
throughput dial: `linger.ms=0` (the default) sends as soon as possible, minimizing per-record latency; a small
positive `linger.ms` (even 5-10ms) lets more records accumulate per batch, which can dramatically increase
throughput for high-volume producers at the cost of a small, bounded delay — Level 2's producer-configuration
lesson measures this tradeoff directly rather than just stating it.

## Common mistakes

- **Assuming distinct keys guarantee distinct partitions.** As this lesson's real output shows directly, that's
  not the contract — only "same key → same partition" is guaranteed; the reverse is a probabilistic outcome of
  the hash function, not a promise.
- **Choosing a key with very low cardinality** (a boolean flag, a two-value status field) **and expecting even
  load distribution.** With few distinct key values, you're entirely at the mercy of how those specific values
  happen to hash — exactly the `alice`/`bob` situation above, but now unavoidable because there's no way to add
  more distinct keys to even things out.
- **Setting `linger.ms` to a large value without measuring the actual latency cost for your use case.** Batching
  helps throughput, but every millisecond of linger is a millisecond every record in that batch waits before
  being sent at all — appropriate for a high-throughput log pipeline, likely wrong for a latency-sensitive
  request/response pattern riding on top of Kafka.

## Exercise

Reproduce this lesson's exact key list (`alice`, `bob`, `carol`, `dave`, `eve`, `frank`, `grace`, `heidi`)
against a topic with a **different** partition count (try 5 instead of 3) and see whether `alice` and `bob`
still collide — predict first, based on the hash-bucket explanation above, then verify. Separately, pick 50
random short strings as keys, produce one record for each, and tally the partition distribution — compare it to
what you'd expect from a uniform random assignment.
