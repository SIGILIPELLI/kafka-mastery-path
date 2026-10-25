---
description: "Real Kafka transactions: a committed vs aborted transaction, a surprising client-buffering gotcha, and proven read_committed/read_uncommitted isolation."
---

# 04 · Exactly-Once Semantics: Idempotence & Transactions

Level 1 showed idempotence preventing duplicate retries within one producer session, one partition. This lesson
goes further with the **transactional** producer API — real commits, a real abort, and a genuinely surprising
discovery about what "aborting a transaction" actually does to data that was never flushed to the broker in the
first place.

## What a transaction actually buys you

A Kafka transaction lets a producer group multiple `send()` calls — potentially across multiple partitions and
topics — into one atomic unit: either all of them become visible to `read_committed` consumers, or none of
them do. This is what makes a true "read from topic A, process, write to topic B" pipeline safe: without
transactions, a crash between writing to B and committing the read offset from A could reprocess or skip
records; with a transaction wrapping both the output write and the offset commit, they succeed or fail
together.

## Worked example: commit, abort, and a real surprise about buffering

```java
producer.beginTransaction();
producer.send(new ProducerRecord<>("tx-topic", "committed-msg-1"));
producer.send(new ProducerRecord<>("tx-topic", "committed-msg-2"));
producer.commitTransaction();

producer.beginTransaction();
producer.send(new ProducerRecord<>("tx-topic", "aborted-msg-1"));
producer.send(new ProducerRecord<>("tx-topic", "aborted-msg-2"));
producer.abortTransaction();          // <- no flush() called first
```

```text
$ bin/kafka-dump-log.sh --files .../tx-topic-0/00000000000000000000.log --print-data-log
baseOffset: 0  ... isTransactional: true  | payload: committed-msg-1
baseOffset: 1  ... isTransactional: true  | payload: committed-msg-2
baseOffset: 2  ... isControl: true        | endTxnMarker: COMMIT
baseOffset: 3  ... isTransactional: true  | payload: committed-msg-3
baseOffset: 4  ... isControl: true        | endTxnMarker: COMMIT
```

(Real output from this course's lab broker, from a real transactional Java producer, compiled and run against
this course's cluster.) Look closely: **there is no trace of `aborted-msg-1`/`aborted-msg-2` anywhere — not
the records, and not even an ABORT marker.** The committed transactions show up exactly as expected, each
followed by its `COMMIT` control record. But the aborted one simply isn't there at all. This isn't a bug in the
dump tool — it's because `send()` on a Kafka producer only **stages** a record in the client-side batching
accumulator (the same batching mechanism from Lesson 2); it doesn't guarantee the record has actually been
transmitted over the network yet. Calling `abortTransaction()` before that buffered batch was ever sent means
the broker never learned this partition was part of the transaction **at all** — there's nothing to mark as
aborted, because nothing arrived.

```java
producer.beginTransaction();
producer.send(new ProducerRecord<>("tx-topic", "aborted-FLUSHED-1"));
producer.send(new ProducerRecord<>("tx-topic", "aborted-FLUSHED-2"));
producer.flush();                     // <- force the batch to actually send
producer.abortTransaction();
```

```text
baseOffset: 5  ... isTransactional: true  | payload: aborted-FLUSHED-1
baseOffset: 6  ... isTransactional: true  | payload: aborted-FLUSHED-2
baseOffset: 7  ... isControl: true        | endTxnMarker: ABORT
```

(Also real.) Forcing a `flush()` before the abort changes the outcome completely: now the records genuinely
reached the broker, got written to the log exactly like any other transactional batch, and **then** a real
`ABORT` control marker was written right after them. The data physically exists in the log either way the
abort happens — the only question is whether the broker ever received it before being told to abort.

## Worked example: proving isolation levels, on the real resulting log

```text
$ # read_committed consumer
committed-msg-1
committed-msg-2
committed-msg-3

$ # read_uncommitted consumer, same topic
committed-msg-1
committed-msg-2
committed-msg-3
aborted-FLUSHED-1
aborted-FLUSHED-2
```

(Real output — two real consumer processes, same topic, different `isolation.level`.) This proves the actual
contract precisely: **`read_committed`** filters out any record belonging to a transaction whose end marker is
`ABORT` (or has no end marker yet at all — an in-progress transaction) — it never saw `aborted-FLUSHED-1/2`.
**`read_uncommitted`** makes no such distinction and returns every physically-written record, abort marker or
not, which is exactly why it showed the flushed-then-aborted messages. Neither isolation level is "the" correct
one universally — `read_committed` is what you want for correctness-sensitive pipelines; `read_uncommitted`
trades that guarantee for being able to see data the instant it's written, without waiting for the
commit/abort decision to resolve.

## How It Actually Works

A transaction is coordinated by a broker-side **transaction coordinator**, tracked against the producer's
`transactional.id`. `beginTransaction()` starts a new transaction epoch; every partition the producer actually
writes to during that transaction gets added to the coordinator's list of "partitions in this transaction";
`commitTransaction()`/`abortTransaction()` tells the coordinator to write end markers to **every partition that
was actually involved** — which is precisely why a partition nothing was ever sent to has no marker at all; it
was never added to that list in the first place. Consumers reading with `read_committed` buffer records from
transactional producers internally and only release them to the application once the corresponding end marker
confirms commit, or discard them silently if the marker says abort (or the producer's session simply ends
without ever committing, handled via transaction timeouts).

## Common mistakes

- **Assuming `abortTransaction()` always leaves a clean, markerless log, with zero trace of the aborted
  data.** As shown above, that's only true if the records were never actually flushed to the broker —
  flushed-then-aborted data is genuinely present on disk, invisible only to `read_committed` consumers, not
  erased.
- **Confusing idempotence (Level 1) with full transactional exactly-once.** Idempotence alone prevents
  duplicate retries to one partition within one producer session; it says nothing about atomicity across
  multiple partitions or topics, which is specifically what the transactional API adds.
- **Using `read_uncommitted` by default "for lower latency" without understanding what it trades away.** It
  can expose data from transactions that are later aborted, or still in progress and might never commit —
  appropriate for some monitoring/debugging use cases, risky for anything that treats what it reads as final.

## Exercise

Write a small transactional producer (or adapt this lesson's) that sends records in a transaction, calls
`flush()`, then aborts. Dump the resulting log segment and confirm you see both the data batch and a real
`ABORT` marker. Then read the topic with both `read_committed` and `read_uncommitted` consumers and confirm
only the latter shows the aborted records — exactly as this lesson's real run did.
