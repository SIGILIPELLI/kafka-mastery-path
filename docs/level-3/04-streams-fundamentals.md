---
description: "KStream vs KTable, demonstrated by a real word-count app whose counts survived a multi-day restart gap because KTable state is durable, not in-memory."
---

# 04 · Kafka Streams Fundamentals: KStream vs KTable

A Kafka Streams application was restarted in this course's own lab, days after it last ran, and its word
counts picked up exactly where they'd left off instead of starting over. That's not a coincidence — it's the
entire point of the `KTable` abstraction, and this lesson explains why by way of that real, unplanned evidence.

## Two views of the same event stream

A **`KStream`** is a stream of independent events — each record is a new, discrete fact, and "updating" a
`KStream` doesn't mean anything; you just get more records. A **`KTable`** is a continuously-updated view of
**latest value per key** — conceptually, and often literally, backed by the same compacted-log model from
Level 1, Lesson 9: a new record for an existing key doesn't append to a growing list, it replaces that key's
current value in the table's view. Aggregation operations (`count()`, `reduce()`, `aggregate()`) on a
`KStream` naturally produce a `KTable`, because "the current count for this word" is exactly a per-key latest
value.

## Worked example: counts that survived days, and a restart, unintentionally

```text
$ echo -e "kafka streams fundamentals\nkstream and ktable\nkafka kafka kafka" | \
    bin/kafka-console-producer.sh --topic streams-plaintext-input --bootstrap-server localhost:9092

$ bin/kafka-console-consumer.sh --topic streams-wordcount-output --from-beginning \
    --bootstrap-server localhost:9092 --property print.key=true \
    --value-deserializer org.apache.kafka.common.serialization.LongDeserializer --max-messages 9
kafka         5
streams       2
fundamentals  1
kstream       1
and           1
ktable        1
kafka         6
kafka         7
kafka         8
```

(Real output from this course's lab broker — genuinely unplanned.) The word `"kafka"` appears **three times**
in the input just produced, which should count up from 1 — instead it starts at **5** and climbs to 6, 7, 8.
That's because this exact Streams application (same `application.id`, `"streams-wordcount"`, the example's
hardcoded default) had been run once already, much earlier in this same lab session, counting `"kafka"` up to
4 at the time. Restarting the application — even after a multi-day gap in this course's own work — picked the
aggregation back up from exactly where it left off, rather than resetting to zero.

## How It Actually Works

A `KTable`'s current state is materialized into a local **state store** (RocksDB by default) on whichever
machine is running that partition's processing — but critically, every update to that state store is also
written to an internal **changelog topic** (visible directly: this app's is named
`streams-wordcount-KSTREAM-AGGREGATE-STATE-STORE-0000000003-changelog`, a real topic this course's broker is
genuinely holding). The local RocksDB store is a *cache* for fast reads; the changelog topic is the durable
source of truth. When a Streams application starts (or restarts, on the same or even a different machine), it
replays its changelog topic to rebuild local state before processing any new input — which is exactly why this
lesson's counts resumed from 5 rather than starting over: the changelog topic had durably recorded "kafka" had
already reached 4, and replaying it on startup reconstructed that state before the three new "kafka" occurrences
were even counted.

This is the direct, practical payoff of treating a `KTable` as log-backed rather than purely in-memory: Streams
applications get the same crash-recovery and rebalancing-safety story Kafka itself has for partition data
(Level 1, Lesson 7) applied to *application* state, not just the raw input stream — a worker crashing and
another instance picking up its partitions doesn't lose the aggregated counts, because they live in Kafka, not
just in one process's memory.

## Common mistakes

- **Expecting a Streams application's aggregated state to reset on restart.** As this lesson shows directly
  (if accidentally), it doesn't by default — if you genuinely want a fresh start, you need to reset the
  application explicitly (the `kafka-streams-application-reset` tool, covered in Level 3's later lessons),
  not just restart the process.
- **Treating a `KStream` and the `KTable` its aggregation produces as the same kind of thing.** They have
  fundamentally different semantics — a `KStream` has no concept of "updating" a prior record; a `KTable` is
  defined entirely around that concept.
- **Forgetting that changelog topics are real, resource-consuming Kafka topics.** They retain data (by
  default, indefinitely, since they're typically compacted rather than time-retained) and count toward real
  disk and replication overhead on the cluster — not a free side effect of using the DSL's aggregation
  operators.

## Exercise

Run any Kafka Streams aggregation example, note its current counts, then genuinely stop and restart the
application (not just the consumer reading its output) and confirm the counts resume rather than reset — as
this lesson's own, unplanned restart did. Then find the changelog topic for your app's state store with
`kafka-topics.sh --list` and confirm its `cleanup.policy` is `compact` (Level 1, Lesson 9) rather than
time-based deletion.
