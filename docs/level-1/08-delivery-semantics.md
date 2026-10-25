---
description: "At-most-once, at-least-once and idempotence explained, with real producer-ID and sequence-number metadata read straight off the disk log."
---

# 08 · Delivery Semantics: At-Most-Once, At-Least-Once & Idempotence

"Exactly once" is the phrase everyone wants and the hardest one to actually deliver honestly. This lesson builds
up to it properly: what a producer can control on its own (at-most-once vs at-least-once), what idempotence adds
on top of that, and real evidence — read straight from the log file — of the mechanism idempotence actually
uses.

## Two honest options, and a third that needs more machinery

**At-most-once**: a producer sends a record and doesn't retry on failure. If the write never reached the
broker, the message is lost — but it's also impossible to accidentally send it twice. **At-least-once**: a
producer retries on any ambiguous failure (a timeout, a dropped connection) until it gets confirmation. This
guarantees the message eventually arrives, but a retry after a message **did** actually succeed (just didn't
confirm fast enough) can produce a real duplicate. Plain retries alone can only give you one or the other —
**idempotence** is the mechanism that lets a producer retry safely without duplicating, which is what lets you
get both "never lost" and "never duplicated" together, at least within a single producer session.

## Worked example: real producer IDs and sequence numbers, read off disk

```text
$ echo -e "msg1\nmsg2\nmsg3" | bin/kafka-console-producer.sh --topic repl-topic --bootstrap-server localhost:9092

$ bin/kafka-dump-log.sh --files /tmp/kafka-cluster/node1/data/repl-topic-0/00000000000000000000.log --print-data-log
baseOffset: 0 lastOffset: 2 count: 3 baseSequence: 0 lastSequence: 2 producerId: 1000 producerEpoch: 0 ...
| offset: 0 ... sequence: 0 ... payload: msg1
| offset: 1 ... sequence: 1 ... payload: msg2
| offset: 2 ... sequence: 2 ... payload: msg3

$ echo "msg4" | bin/kafka-console-producer.sh --topic repl-topic --bootstrap-server localhost:9092
$ bin/kafka-dump-log.sh --files /tmp/kafka-cluster/node1/data/repl-topic-0/00000000000000000000.log --print-data-log
baseOffset: 0 ... producerId: 1000 producerEpoch: 0 ...   <- unchanged, first producer session
baseOffset: 3 lastOffset: 3 count: 1 baseSequence: 0 lastSequence: 0 producerId: 0 producerEpoch: 0 ...
| offset: 3 ... sequence: 0 ... payload: msg4
```

(Real output from this course's lab broker — nothing invented, these are exactly the fields the broker wrote.)
Every record batch carries a **`producerId`** and a per-producer **`sequence`** number, starting at 0 for that
producer and incrementing with every batch it sends. The console producer tool enables idempotence **by
default** (it has been the client default since Kafka 3.0), which is exactly why this metadata exists at all
even though nothing in the commands above asked for it explicitly. Notice the *second* producer invocation
(`msg4`) got a **different** `producerId` (`0` instead of `1000`) and restarted its sequence at `0` — each new
producer *session* (a new client process, in this case) is issued a fresh producer ID by the broker's
transaction coordinator; the sequence numbering that enables duplicate detection is scoped to one producer ID's
lifetime, not to the topic or partition as a whole.

## How It Actually Works

With idempotence enabled, the broker tracks the **last sequence number successfully written for each
(producerId, partition) pair**. When a batch arrives, the broker checks whether its sequence number is exactly
one more than the last one it accepted for that producer ID. If the producer retries a batch that **did**
already succeed (the first acknowledgment was simply lost in transit), the retry arrives with the **same**
sequence number the broker already has recorded — the broker recognizes this as a duplicate of an
already-committed batch and returns success without writing it again. If a genuinely new batch arrives with the
expected next sequence number, it's written normally. This is a purely broker-side, per-producer-session
bookkeeping trick — no distributed coordination, no two-phase commit, just a running counter the broker checks —
which is exactly why idempotence has very low overhead and is on by default.

Idempotence alone guarantees **no duplicates from retries within one producer session, to one partition**. It
does **not**, by itself, give you atomicity across multiple partitions or multiple topics in one logical
operation (writing to an "orders" topic and an "inventory" topic as one atomic unit, for instance) — that's
what the **transactional** producer API adds on top (Level 2, Lesson 4), using a transaction coordinator and a
two-phase-commit-style protocol to make a whole batch of writes across multiple partitions succeed or fail
together, which is the actual mechanism behind Kafka's "exactly-once" marketing claim for multi-partition,
multi-topic operations.

## Common mistakes

- **Assuming idempotence protects against application-level duplicate sends**, like a user double-clicking
  "submit" and your code genuinely calling `producer.send()` twice with different content. Idempotence
  deduplicates retries of the *same* underlying request at the client-library level; it has no idea two
  separate `send()` calls represent the same business event unless your own application logic makes them
  identical at that level.
- **Thinking "exactly once" means no possible duplicates anywhere in the pipeline.** It specifically covers
  producer-to-broker delivery (with idempotence/transactions) and, separately, consumer processing when done
  correctly (reading and committing offsets transactionally alongside any output writes) — a consumer that
  reprocesses after a crash without transactional coordination can still observe or produce duplicates
  downstream.
- **Disabling idempotence "for performance" without measuring.** The overhead is genuinely small (one sequence
  check per batch) and the correctness it buys is significant — turning it off is rarely the right lever for a
  real performance problem.

## Exercise

Produce a batch of messages with the console producer, dump the resulting log segment, and note the
`producerId` and `sequence` values. Restart the console producer (ending that session) and produce more
messages to the same topic — confirm you get a **new** `producerId` and that its sequence starts again at `0`,
exactly as this lesson's real run did.
