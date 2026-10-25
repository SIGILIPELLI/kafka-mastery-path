---
description: "What Kafka actually is: a partitioned, durable, append-only log, and why that model behaves so differently from a traditional message queue."
---

# 01 · What Kafka Actually Is: The Log-Based Messaging Model

If you've used a traditional message queue (RabbitMQ, SQS, ActiveMQ), the first thing that trips people up about
Kafka is that it doesn't delete a message once someone reads it. That single difference — a message staying in
place after delivery — is the clue to what Kafka actually is underneath the "messaging system" label: a
**distributed, append-only log**, not a queue at all.

## A topic is a log, split into partitions

A **topic** is a named stream of records. Internally, a topic is split into one or more **partitions**, and each
partition is, physically, an append-only log file on disk: new records are always written at the end, records
are never modified in place, and each record gets a sequential **offset** — its position in that specific
partition's log, starting at 0 and counting up forever (or until the record ages out, Lesson 9). Order is
**only** guaranteed within a single partition — Kafka makes no promise about the relative order of records in
different partitions of the same topic, which is the single most common source of "why did my events arrive out
of order" confusion for people used to a single FIFO queue.

## Worked example: a real cluster, a real topic, a real version check

```text
$ bin/kafka-topics.sh --version
4.4.0

$ bin/kafka-storage.sh random-uuid
DfcdfOpkTYe9c0fXQmCgxQ

$ bin/kafka-storage.sh format -t DfcdfOpkTYe9c0fXQmCgxQ -c config/server.properties --standalone
Formatting dynamic metadata voter directory /tmp/kraft-combined-logs with metadata.version 4.4-IV2.

$ bin/kafka-server-start.sh config/server.properties
...
[...] INFO Kafka version: 4.4.0 ...
[...] INFO Awaiting socket connections on 0.0.0.0:9092. (kafka.network.DataPlaneAcceptor)

$ bin/kafka-topics.sh --create --topic demo-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
Created topic demo-topic.

$ bin/kafka-topics.sh --describe --topic demo-topic --bootstrap-server localhost:9092
Topic: demo-topic	TopicId: _BAk3JKEQbWTWteNuFOUYw	PartitionCount: 3	ReplicationFactor: 1	Configs: min.insync.replicas=1,...
	Topic: demo-topic	Partition: 0	Leader: 1	Replicas: 1	Isr: 1
	Topic: demo-topic	Partition: 1	Leader: 1	Replicas: 1	Isr: 1
	Topic: demo-topic	Partition: 2	Leader: 1	Replicas: 1	Isr: 1
```

(Real output from this course's lab broker — Kafka 4.4.0, the current release at the time this course was
written, running entirely without ZooKeeper.) `kafka-storage.sh format` is a one-time step that initializes this
broker's on-disk metadata before it can start at all — notice it mentions `metadata.version 4.4-IV2`, a version
for the cluster's internal metadata format, separate from the Kafka software version itself. `--create` with
`--partitions 3` splits `demo-topic` into three independent logs from the start, and `--describe` confirms each
partition has its own **leader** (broker `1` for all three here, since this lab runs a single broker) and its own
**ISR** (in-sync replica set, Lesson 7) — each partition is tracked and replicated completely independently of
its siblings.

## How It Actually Works

The append-only, immutable-once-written design is what makes several of Kafka's defining properties possible at
once. Because records are never modified, multiple consumers can read the same partition at their own pace
without any locking or coordination — a consumer reading offset 1,000 doesn't block, or get blocked by, another
consumer reading offset 50,000 of the same partition; each just tracks its own position. Because a producer only
ever **appends**, writes are sequential disk I/O (fast, even on spinning disks, because there's no seeking around
to overwrite existing data) rather than random-access updates. And because nothing is deleted on read, a topic
can serve both "give me only new messages from now on" and "replay everything from the beginning" use cases from
the exact same data, which a traditional destroy-on-consume queue structurally cannot do — once a queue message
is acknowledged and removed, a second consumer that wants to reprocess history has nothing left to read.

Partitioning is also the unit of **parallelism** and the unit of **ordering** at the same time, and that pairing
is deliberate: Kafka could have offered strict ordering across an entire topic, but only by serializing all
writes and reads through one partition, which would cap throughput at whatever a single disk and a single
consumer thread could handle. Splitting into partitions lets multiple producers write and multiple consumers
read in parallel — real horizontal scalability — at the cost of only guaranteeing order *within* each partition.
This is why key-based partitioning (Lesson 4) matters so much in practice: if you need all events for one
customer, one device, or one order to stay in order relative to each other, you need them routed to the *same*
partition, not spread arbitrarily across all of them.

## Common mistakes

- **Expecting global ordering across an entire topic.** Kafka guarantees order within a partition only — if
  your application logic silently assumes topic-wide ordering, it will work fine with one partition and then
  mysteriously reorder itself the moment you scale to more.
- **Treating Kafka as "just a faster queue" and missing the replay capability.** Because records aren't deleted
  on consumption, a new consumer group can read a topic from the very beginning at any time — a capability
  traditional queues don't offer, and a common reason teams reach for Kafka specifically for use cases needing
  replay, audit, or multiple independent consumers of the same event stream.
- **Assuming "the log" is one single physical file.** As the `--describe` output above shows, it's one log
  *per partition*, each independently tracked, leader-assigned, and replicated — "the topic's log" is really a
  set of separate logs working together.

## Exercise

Create a topic with 4 partitions, then produce 20 messages to it **without** specifying a key (so they land on
partitions essentially at random). Use `kafka-console-consumer.sh --property print.partition=true` to see which
partition each message landed on, and count how evenly they spread. Then predict, before running it: if you
instead produced all 20 messages with the *same* key, which partition would they all land on, and why — confirm
your prediction with the same consumer command.
