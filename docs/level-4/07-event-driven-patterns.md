---
description: "A real transactional outbox proven across two Kafka topics: a committed transaction appears in both, an aborted one appears in neither."
---

# 07 · Event-Driven Patterns: Event Sourcing, CQRS & the Outbox

Three architectural patterns come up constantly once Kafka is the backbone of a system: event sourcing, CQRS,
and the outbox pattern. This lesson proves the trickiest one — the outbox pattern's core atomicity guarantee —
directly, using the real transactional producer from Level 2, Lesson 4, applied across two independent topics.

## The problem the outbox pattern solves

A service that updates its own state *and* needs to tell the rest of the system "this happened" faces a real
atomicity problem: if it writes its state update and then separately publishes an event, a crash between the
two leaves them inconsistent — state changed, nobody told; or an event published for a state change that
never actually committed. The **outbox pattern**'s answer, when Kafka is the event bus, is to make both writes
part of **one Kafka transaction** — exactly the mechanism Level 2, Lesson 4 demonstrated, just applied here to
two application-meaningful topics instead of one.

## Worked example: a real atomic write, and a real atomic non-write

```java
producer.beginTransaction();
producer.send(new ProducerRecord<>("orders-table", "order-123", "status=PLACED,amount=250"));
producer.send(new ProducerRecord<>("order-events-outbox", "order-123", "OrderPlaced:order-123:250"));
producer.commitTransaction();

producer.beginTransaction();
producer.send(new ProducerRecord<>("orders-table", "order-456", "status=PLACED,amount=999"));
producer.send(new ProducerRecord<>("order-events-outbox", "order-456", "OrderPlaced:order-456:999"));
producer.flush();
producer.abortTransaction();
```

```text
--- orders-table (read_committed) ---
order-123 -> status=PLACED,amount=250

--- order-events-outbox (read_committed) ---
order-123 -> OrderPlaced:order-123:250
```

(Real output from this course's lab broker — a genuine transactional producer writing to two real, separate
topics.) `order-123`'s transaction committed, and it shows up in **both** topics — the "state" topic and the
"event" topic — consistently. `order-456`'s transaction was aborted, and it shows up in **neither** topic —
not a half-written state with a missing event, not an event with no corresponding state, but a clean,
complete absence from both, read with `read_committed` isolation. This is the entire outbox guarantee, proven
directly: the state change and the notification event either both become visible together, or neither does,
with no possible in-between state a consumer could observe.

## How It Actually Works

This works for exactly the reason Level 2, Lesson 4 explained: a Kafka transaction's atomicity isn't scoped to
one partition or one topic — it spans every partition the producer actually writes to during that
transaction, with the transaction coordinator tracking all of them and writing commit/abort markers to each
one together. When "the database" in a real outbox setup actually *is* a Kafka topic (as this lesson's
`orders-table` simulates), the outbox pattern reduces directly to "use a Kafka transaction" — no polling
pattern, no separate outbox table, no CDC connector needed, because Kafka's own transactional guarantee
already covers it. The classical "outbox table in a relational database, polled by a CDC connector into
Kafka" version of this pattern exists specifically for services whose **real** system of record is a
traditional database, not Kafka itself — there, the atomicity is achieved by writing both the business row and
the outbox event row in one local database transaction (ordinary ACID, no Kafka involved yet), and a
separate CDC process (often Kafka Connect with a Debezium-style connector) reliably streams the outbox
table's rows into Kafka afterward.

**Event sourcing** and **CQRS** build on the same durable-log foundation from a different angle: event
sourcing treats the sequence of events (not a current-state snapshot) as the source of truth, rebuildable at
any time by replaying the log — directly analogous to Level 3's `KTable` state stores replaying their
changelog topics. **CQRS** (Command Query Responsibility Segregation) separates the model used to accept
writes from the model(s) used to serve reads, often using Kafka Streams-style materialized views (Level 3,
Lesson 4's `KTable`s) as the read side, kept continuously up to date from the same event stream the write side
produces.

## Common mistakes

- **Assuming "the outbox pattern" always means a polling-based CDC setup.** As this lesson shows, when Kafka
  itself is both the state store and the event bus, a Kafka transaction *is* the outbox pattern — the more
  elaborate CDC version is specifically for bridging a non-Kafka system of record into Kafka.
- **Forgetting `read_committed` isolation matters for consumers of outbox-pattern topics.** A consumer reading
  with `read_uncommitted` could observe a state write or event from a transaction that later aborts — exactly
  the inconsistency the pattern exists to prevent, reintroduced by the wrong isolation level.
- **Treating event sourcing as "just keep every event forever" without a replay/rebuild story.** The value of
  event sourcing is specifically the ability to *reconstruct* state from the log — a design that stores events
  but never actually exercises replaying them to rebuild state hasn't really adopted the pattern, just
  renamed "logging."

## Exercise

Reproduce this lesson's exact outbox test: a transactional producer writing to two topics, one committed and
one aborted, read back with `read_committed` isolation. Then extend it with a third topic and confirm the same
all-or-nothing guarantee holds across three topics in a single transaction, not just two.
