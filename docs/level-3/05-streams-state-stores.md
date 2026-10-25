---
description: "Streams state stores explained by reading a real changelog topic directly — it holds the exact same aggregated values as the materialized KTable."
---

# 05 · Streams Stateful Operations & State Stores

Lesson 4 showed a `KTable`'s aggregated counts surviving a real restart because of a changelog topic. This
lesson reads that changelog topic **directly**, independent of the application, to prove it genuinely is the
full, standalone source of truth for the aggregation's state — not just an internal implementation detail you
have to trust blindly.

## What a state store actually is

Any stateful DSL operation — `count()`, `reduce()`, `aggregate()`, a windowed aggregation, a join needing to
remember one side's records — needs somewhere to keep that state between records. That's a **state store**:
by default, an embedded RocksDB instance local to the task processing that partition, chosen specifically
because it's an efficient, embeddable key-value store that doesn't require running a separate database
process. Every state store Kafka Streams manages is, by default, **backed by a changelog topic** — the
mechanism Lesson 4 relied on for its unplanned restart-recovery story.

## Worked example: reading the changelog directly, bypassing the application entirely

```text
$ bin/kafka-topics.sh --list --bootstrap-server localhost:9092 | grep changelog
streams-wordcount-KSTREAM-AGGREGATE-STATE-STORE-0000000003-changelog

$ bin/kafka-console-consumer.sh \
    --topic streams-wordcount-KSTREAM-AGGREGATE-STATE-STORE-0000000003-changelog \
    --from-beginning --bootstrap-server localhost:9092 --property print.key=true \
    --value-deserializer org.apache.kafka.common.serialization.LongDeserializer --max-messages 8
kafka         5
streams       2
fundamentals  1
kstream       1
and           1
ktable        1
kafka         6
kafka         7
```

(Real output from this course's lab broker — read with a plain console consumer, with no Streams application
involved in this specific read at all.) This is **exactly** the same sequence of values Lesson 4's read of the
actual output topic showed. That's not a coincidence of this particular example — for a simple aggregation
like word count, the changelog topic and the externally-visible output topic end up carrying equivalent
data, because the aggregation's result *is* the thing being both materialized to the state store and emitted
downstream. Reading the changelog with a plain consumer, bypassing the Streams application and its RocksDB
store entirely, still gives you the full, correct picture of "what does this KTable currently believe," which
is the concrete proof that the changelog — not the local RocksDB files — is the real, durable source of truth.

## How It Actually Works

When a Streams task updates its local RocksDB state store (incrementing a word's count, say), it **also**
produces a record to that store's changelog topic recording the update, as part of the same processing step.
This dual-write is what lets a completely fresh instance (a new machine, after `state.dir` is wiped, or after
this lesson's own multi-day gap) **rebuild** an equivalent RocksDB store from scratch: on startup, the task
reads its changelog topic from the beginning (or from a local checkpoint, if one survived) and replays every
update into a blank RocksDB instance before resuming live processing — which is precisely the mechanism Lesson
4's surprising "kafka started at 5, not 1" result depended on.

This also explains why changelog topics default to **compacted** cleanup policy (Level 1, Lesson 9): a
changelog only needs to retain the *latest* value for each key to successfully rebuild current state — the
full history of every intermediate count isn't needed for recovery, only "what's the final value for this
key right now," which is exactly log compaction's job.

## Common mistakes

- **Treating the local RocksDB directory as the real data and the changelog as a side effect.** As
  demonstrated directly, it's the other way around: the changelog is durable and authoritative; RocksDB is a
  disposable, rebuildable local cache for fast reads.
- **Deleting a changelog topic manually to "clean up" a Streams application**, without understanding this
  destroys the only durable record of that application's current aggregated state — any surviving or future
  instance rebuilding state from it will come up empty or inconsistent.
- **Assuming every state store's changelog content matches its downstream output topic exactly**, as this
  lesson's simple word-count example happens to. For a windowed aggregation or a join, the changelog and any
  externally-emitted stream can diverge in format and content — this lesson's clean correspondence is a
  property of word count's particular simplicity, not a universal guarantee.

## Exercise

Find a stateful Kafka Streams application's changelog topic with `kafka-topics.sh --list` (look for the
`-changelog` suffix) and read it directly with a plain console consumer, independent of the running
application. Confirm the values you see match what the application itself reports as current state (through
its output topic, a queryable state store, or logs) — direct, hands-on proof of where the real state actually
lives.
