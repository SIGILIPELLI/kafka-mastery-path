---
description: "Project: three fully concurrent producers writing to the same topic, proving per-key ordering survives real concurrency when keys route to partitions."
---

# 10 · Project: A Producer/Consumer Pipeline with Partition Keys

Every lesson in this level built one piece of the model: partitions are independent logs, keys decide which
partition a record lands on, and ordering is only guaranteed within a partition. This project puts all three
claims under real pressure at once: three genuinely concurrent, independent producer processes, writing
interleaved events for three different keys, at the same time, to the same topic.

## The setup

```bash
kafka-topics.sh --create --topic orders --bootstrap-server localhost:9092 --partitions 4 --replication-factor 1
```

Three "customers" — `customerA`, `customerB`, `customerC` — each have 5 sequential events. If this were a real
application, these might be three different HTTP request handlers, each independently producing events for
*their own* customer, with no coordination between them and no shared knowledge of what the others are doing
at the same instant.

```bash
kafka-console-producer.sh --topic orders --bootstrap-server localhost:9092 \
  --property "parse.key=true" --property "key.separator=:" < prodA.txt &
kafka-console-producer.sh --topic orders --bootstrap-server localhost:9092 \
  --property "parse.key=true" --property "key.separator=:" < prodB.txt &
kafka-console-producer.sh --topic orders --bootstrap-server localhost:9092 \
  --property "parse.key=true" --property "key.separator=:" < prodC.txt &
wait
```

Three separate OS processes, launched at essentially the same moment, each independently calling the Kafka
producer API with no awareness of the other two.

## What actually came out the other end

```text
$ bin/kafka-console-consumer.sh --topic orders --from-beginning --bootstrap-server localhost:9092 \
    --property print.partition=true --property print.key=true --max-messages 15
Partition:0  customerB  event-1
Partition:0  customerB  event-2
Partition:0  customerB  event-3
Partition:0  customerB  event-4
Partition:0  customerB  event-5
Partition:2  customerA  event-1
Partition:2  customerA  event-2
Partition:2  customerA  event-3
Partition:2  customerA  event-4
Partition:2  customerA  event-5
Partition:3  customerC  event-1
Partition:3  customerC  event-2
Partition:3  customerC  event-3
Partition:3  customerC  event-4
Partition:3  customerC  event-5
```

(Real output from this course's lab broker — three genuinely independent, concurrently-running producer
processes, not a simulation.) Every single prediction from this level holds up under real concurrency: each
customer's key routed to **exactly one** partition — `customerB` to partition 0, `customerA` to partition 2,
`customerC` to partition 3 — and **within each partition, events 1 through 5 arrived in exact order**, despite
the three producers racing each other with zero coordination. No partition received a mix of customers (keys
don't collide here, unlike Lesson 4's `alice`/`bob` coincidence — these three happened to spread cleanly across
four partitions). This is the entire payoff of key-based partitioning stated as a provable fact instead of a
claim: *order-per-key* survives real-world concurrent, uncoordinated producers, specifically because ordering
is a per-partition property, and same-key records always serialize onto the same partition regardless of which
process happens to send them or in what order those processes happen to be scheduled by the OS.

## How It Actually Works

Nothing about this required locking, coordination, or awareness between the three producer processes. Each one
independently computed `hash(key) % numPartitions` for its own events and sent them to whichever broker leads
that resulting partition — the **broker**, not the producers, is what serializes the actual writes to each
partition's log, by virtue of being the single leader every write for that partition routes through. This is
the real mechanism behind Kafka's claim of "scalable with per-key ordering": you get genuine horizontal
producer scalability (as many independent producer processes as you want, no shared lock between them) *and*
strict per-key ordering, because the partition-routing step plus single-leader-per-partition writing is what
does the serializing, not any cooperation between producers.

If this project's events had been produced **without keys** instead, the sticky default partitioner (Lesson 4)
would have spread them across partitions by batch rather than by customer — meaning two events for the *same*
customer, sent in two different batches, could easily land on different partitions with no ordering guarantee
between them at all. The entire reason this project's output stays cleanly ordered per customer is the explicit
key, not an accident of timing.

## Common mistakes

- **Assuming this project's clean per-customer partition split is guaranteed rather than a hash outcome.**
  As Lesson 4's `alice`/`bob` collision showed, different keys can land on the same partition by chance — this
  project's three customers happening to spread across three different partitions is a real result, not a
  structural promise the API makes.
- **Running concurrent producers without keys and expecting the same ordering guarantee.** As explained
  above, unkeyed records use sticky, per-batch partitioning — two events for the same logical entity, sent in
  separate batches, have no guaranteed relative order without an explicit key tying them to one partition.
- **Treating "no coordination needed between producers" as "no partitioning logic happens at all."** Each
  producer still independently computes the same deterministic `hash(key) % numPartitions` — the lack of
  coordination is possible *because* that computation is deterministic and identical everywhere, not because
  partitioning is skipped.

## Exercise

Reproduce this project with a fourth customer whose key happens to hash to the same partition as one of the
other three (try a few short names and check with `kafka-console-consumer.sh --property print.partition=true`
until you find a collision, the way Lesson 4 found one for `alice`/`bob`). Confirm that even sharing a
partition, each customer's own events still arrive in order — but that two *different* customers' events on
that same shared partition can now interleave with each other in arrival order, which is expected and harmless
as long as your application only needs ordering *within* a key, not across keys.
