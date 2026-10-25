---
description: "Kafka Level 1: what Kafka is, brokers and KRaft, topics/partitions, producers, consumer groups, replication, delivery semantics and retention."
---

# Level 1 · The Log, Topics & Partitions

Level 1 builds the one mental model everything else in this course depends on: a topic is a partitioned, durable,
append-only log, and almost every Kafka behavior — ordering, delivery guarantees, consumer group rebalancing,
replication — falls directly out of that one fact. Every command shown here was run against a real, local,
single-broker Kafka cluster.

!!! note "What was run, and what wasn't"
    This course runs a real Apache Kafka 4.4.0 broker in KRaft mode (no ZooKeeper — removed entirely as of Kafka
    4.0) on a laptop, using the bundled command-line tools (`kafka-topics.sh`, `kafka-console-producer.sh`,
    `kafka-console-consumer.sh`, `kafka-consumer-groups.sh`). Every piece of terminal output in this level is real,
    observed output from that broker. Where a lesson describes behavior at a scale or in a configuration this
    single-broker lab couldn't reproduce (a true multi-datacenter outage, for instance), it says so plainly.

**Prerequisites:** basic command-line comfort and a general sense of client-server networking. No prior
messaging-system experience assumed. You need Java 17 or newer to run the labs yourself.

## Modules

1. **[What Kafka Actually Is](01-what-kafka-is.md)**: the log-based messaging model, and how it differs from a
   traditional queue.
2. **[Brokers, Clusters & KRaft](02-brokers-clusters-kraft.md)**: what a broker does, and how the KRaft metadata
   quorum replaced ZooKeeper.
3. **[Topics & Partitions](03-topics-partitions-log.md)**: how the log is really laid out on disk, segment by
   segment.
4. **[Producers: Keys, Partitioning & Batching](04-producers-keys-batching.md)**: how a key decides a partition,
   and why batching matters.
5. **[Consumers & Consumer Groups](05-consumers-consumer-groups.md)**: offsets, partition assignment, and why two
   consumers in a group never double-read a partition.
6. **[The New Consumer Rebalance Protocol (KIP-848) vs Classic](06-rebalance-protocols.md)**: eager vs
   cooperative vs the new incremental protocol.
7. **[Replication: Leaders, Followers & the ISR](07-replication-isr.md)**: how Kafka survives a broker failure
   without losing committed data.
8. **[Delivery Semantics](08-delivery-semantics.md)**: at-most-once, at-least-once, and what idempotence actually
   buys you.
9. **[Retention & Compaction](09-retention-compaction.md)**: time/size-based deletion vs log compaction, and when
   to use which.
10. **[Project: A Producer/Consumer Pipeline with Partition Keys](10-project-producer-consumer-pipeline.md)**: a
    real, runnable pipeline that proves ordering-per-key.

## After Level 1

Level 2 moves from using a cluster to operating one: tuning producer and consumer configuration for real, exactly-once
semantics with transactions, the metrics that actually matter, log internals in practice, security, and scaling a
cluster.
