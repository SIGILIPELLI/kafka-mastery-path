---
title: "Kafka Interview Questions and Answers (179 Questions)"
description: "179 Kafka interview questions with answers and explanations, from beginner to advanced, organised by level. Free, from the Kafka Mastery Path."
---

# Kafka Interview Questions and Answers

**179 questions** with the correct answer and a short explanation, organised from beginner (Level 1) to advanced (Level 4). Every question comes from a lesson in the free Kafka Mastery Path, so you can open the lesson to go deeper.

!!! tip "Practise, don't just read"
    Each lesson also ends with an interactive quiz, and the [final quiz](quiz.md) tests all four levels. Try to answer each question below before reading the answer.

## Level 1 · The Log, Topics & Partitions

### [01 · What Kafka Actually Is: The Log-Based Messaging Model](level-1/01-what-kafka-is.md)

**Q1.** What is Kafka at its core?

**Answer:** A distributed, durable, append-only commit log for streaming events

Producers write, consumers read at their own pace.

**Q2.** How does Kafka differ from a traditional queue?

**Answer:** Messages are retained and can be re-read by many independent consumers

Consumers track offsets.

**Q3.** What problem does Kafka solve between systems?

**Answer:** Decoupling producers from consumers with scalable, replayable event streams

Integration backbone.

**Q4.** Is Kafka's default delivery model push or pull?

**Answer:** Pull: consumers fetch at their own rate

Natural backpressure.

### [02 · Brokers, Clusters &amp; KRaft](level-1/02-brokers-clusters-kraft.md)

**Q5.** What does a Kafka broker do?

**Answer:** Stores partitions and serves produce/fetch requests

A cluster is many brokers.

**Q6.** What replaced ZooKeeper for metadata?

**Answer:** KRaft: Raft-based controller quorum inside Kafka

No external ZooKeeper.

**Q7.** How many controllers keep a quorum with 3 voters?

**Answer:** 2 (a majority)

Tolerates one failure.

**Q8.** What's a cluster ID for?

**Answer:** Uniquely identifying the cluster and formatting KRaft storage

kafka-storage format.

### [03 · Topics &amp; Partitions: How the Log Is Really Stored](level-1/03-topics-partitions-log.md)

**Q9.** What is a topic partition?

**Answer:** An ordered, immutable log; the unit of parallelism and ordering

Order only within a partition.

**Q10.** What is an offset?

**Answer:** A message's sequential position within a partition

Per partition.

**Q11.** Where is ordering guaranteed?

**Answer:** Within a single partition only

Key by entity.

**Q12.** With 6 partitions and a consumer group of 8 consumers, how many consumers are idle?

```python
print(max(0,8-6))
```

**Answer:** 2

Parallelism is bounded by partitions. Verified by running it.

### [04 · Producers: Keys, Partitioning &amp; Batching](level-1/04-producers-keys-batching.md)

**Q13.** Which partition does a key map to with hash % 4 (simple model)? Keys 'a','b','c' with ord sums 97,98,99.

```python
print([k%4 for k in (97,98,99)])
```

**Answer:** [1, 2, 3]

Same key, same partition. Verified by running it.

**Q14.** Why set a record key?

**Answer:** To keep related events ordered in the same partition

Keyed partitioning.

**Q15.** What do linger.ms and batch.size trade?

**Answer:** Latency versus throughput via batching

Bigger batches, higher throughput.

**Q16.** What does compression do for producers?

**Answer:** Shrinks batches to save network and disk

lz4/zstd.

### [05 · Consumers &amp; Consumer Groups](level-1/05-consumers-consumer-groups.md)

**Q17.** What is a consumer group?

**Answer:** Consumers sharing a topic's partitions, each partition read by one member

Scale-out.

**Q18.** What does committing an offset record?

**Answer:** The consumer's progress so it can resume

__consumer_offsets.

**Q19.** What if two groups subscribe to one topic?

**Answer:** Each gets every message independently

Fan-out.

**Q20.** What happens with more consumers than partitions?

**Answer:** Extra consumers sit idle

Plan partitions.

### [06 · The New Consumer Rebalance Protocol (KIP-848) vs Classic](level-1/06-rebalance-protocols.md)

**Q21.** What triggers a rebalance?

**Answer:** Members joining/leaving or partition changes

Pauses consumption.

**Q22.** What does cooperative rebalancing improve?

**Answer:** Only moved partitions pause instead of stop-the-world

Incremental.

**Q23.** What does static membership (group.instance.id) reduce?

**Answer:** Rebalances on short restarts

Sticky identity.

**Q24.** What does session.timeout.ms control?

**Answer:** How long before a silent member is considered dead

Heartbeats.

### [07 · Replication: Leaders, Followers &amp; the ISR](level-1/07-replication-isr.md)

**Q25.** What is the ISR?

**Answer:** In-sync replicas that are caught up with the leader

Candidates for leader.

**Q26.** What does replication factor 3 give?

**Answer:** Two additional copies, tolerating broker failures

Durability.

**Q27.** What does min.insync.replicas=2 with acks=all guarantee?

**Answer:** A write succeeds only when at least 2 replicas have it

Fail instead of losing data.

**Q28.** What happens to a lagging follower?

**Answer:** It leaves the ISR until it catches up

replica.lag.time.max.

### [08 · Delivery Semantics: At-Most-Once, At-Least-Once &amp; Idempotence](level-1/08-delivery-semantics.md)

**Q29.** What does acks=all mean?

**Answer:** The leader waits for all in-sync replicas

Strongest durability.

**Q30.** What do at-least-once semantics risk?

**Answer:** Duplicates after retries

Idempotent consumers.

**Q31.** What does an idempotent producer prevent?

**Answer:** Duplicates from retries within a partition

enable.idempotence.

**Q32.** What's at-most-once?

**Answer:** No retries and no duplicates, but data may be lost

Rarely wanted.

### [09 · Retention &amp; Compaction](level-1/09-retention-compaction.md)

**Q33.** What does retention.ms control?

**Answer:** How long data is kept before deletion

Time-based.

**Q34.** What does log compaction keep?

**Answer:** The latest value per key

Changelogs.

**Q35.** How is a key deleted in a compacted topic?

**Answer:** Write a tombstone (null value)

Later cleaned.

**Q36.** When use compaction instead of deletion?

**Answer:** State/changelog topics where only the latest value matters

Different goals.

### [10 · Project: A Producer/Consumer Pipeline with Partition Keys](level-1/10-project-producer-consumer-pipeline.md)

**Q37.** What does the pipeline project combine?

**Answer:** A producer, topic and consumer group with offsets and delivery settings

Level 1 project.

**Q38.** How verify no loss?

**Answer:** Compare produced and consumed counts end to end

Reconcile.

**Q39.** What do you handle on consumer failure?

**Answer:** Restart from committed offsets, with idempotent processing

At-least-once.

**Q40.** Why commit after processing?

**Answer:** To avoid losing messages on crash (at-least-once)

Order matters.

## Level 2 · Operating a Cluster

### [01 · Multi-Broker Clusters &amp; the KRaft Quorum in Practice](level-2/01-multi-broker-kraft-quorum.md)

**Q41.** How many brokers tolerate one failure with replication factor 3?

**Answer:** At least 3

Replicas on different brokers.

**Q42.** What is a KRaft quorum?

**Answer:** Controllers electing a leader and replicating metadata

Raft.

**Q43.** Why odd numbers of controllers?

**Answer:** Majorities are well-defined and cheaper

3 or 5.

**Q44.** What does the active controller do?

**Answer:** Manages partition leadership and metadata changes

Cluster brain.

### [02 · Producer Configuration That Actually Matters](level-2/02-producer-configuration.md)

**Q45.** What does enable.idempotence do?

**Answer:** Deduplicates producer retries via sequence numbers

Safe retries.

**Q46.** Which setting bounds total send time?

**Answer:** delivery.timeout.ms

Includes retries.

**Q47.** What does max.in.flight.requests matter for?

**Answer:** Ordering when retries occur (idempotence allows up to 5)

Order safety.

**Q48.** What does buffer.memory limit?

**Answer:** Unsent records buffered in the producer

Blocks when full.

### [03 · Consumer Configuration &amp; Fetch Tuning](level-2/03-consumer-configuration.md)

**Q49.** What does auto.offset.reset=earliest do?

**Answer:** Start from the beginning when no committed offset exists

New groups.

**Q50.** What does max.poll.interval.ms guard?

**Answer:** Time between polls before the consumer is considered stuck

Slow processing.

**Q51.** What does fetch.min.bytes trade?

**Answer:** Throughput versus latency

Broker waits for data.

**Q52.** Why disable auto-commit in critical apps?

**Answer:** Commit only after processing to control delivery semantics

Manual commits.

### [04 · Exactly-Once Semantics: Idempotence &amp; Transactions](level-2/04-exactly-once-transactions.md)

**Q53.** What do transactions enable?

**Answer:** Atomic writes across partitions plus offset commits (exactly-once in Kafka pipelines)

transactional.id.

**Q54.** What must consumers set to see only committed data?

**Answer:** isolation.level=read_committed

Skip aborted.

**Q55.** What does exactly-once cover?

**Answer:** Kafka-to-Kafka read-process-write, not external side effects

Idempotent sinks.

**Q56.** What is a zombie producer fenced by?

**Answer:** The transactional.id epoch

Fencing.

### [05 · Monitoring: The Metrics That Actually Matter](level-2/05-monitoring-metrics.md)

**Q57.** What's the key consumer health metric?

**Answer:** Consumer lag (end offset minus committed offset)

Growing lag means falling behind.

**Q58.** What does UnderReplicatedPartitions indicate?

**Answer:** Followers not keeping up with leaders

Alert.

**Q59.** Which metric signals broker request saturation?

**Answer:** Request handler idle ratio dropping

Capacity.

**Q60.** What should alerts target?

**Answer:** Symptoms like lag growth and ISR shrinks

Actionable.

### [06 · Log Segments, Indexes &amp; Cleanup in Practice](level-2/06-segments-indexes-cleanup.md)

**Q61.** What is a log segment?

**Answer:** A file chunk of a partition log; old segments are deleted or compacted

segment.bytes.

**Q62.** What do index files do?

**Answer:** Map offsets/timestamps to file positions for fast lookup

.index/.timeindex.

**Q63.** Why does a small segment.bytes cost?

**Answer:** Many files and slower retention granularity

Balance.

**Q64.** When is data eligible for deletion?

**Answer:** When whole segments exceed retention

Segment granularity.

### [07 · Security: SASL, SSL &amp; ACLs](level-2/07-security-sasl-ssl-acls.md)

**Q65.** What does TLS (SSL) protect?

**Answer:** Data in transit between clients and brokers

Encryption + auth.

**Q66.** What does SASL provide?

**Answer:** Authentication mechanisms (SCRAM, OAUTHBEARER, GSSAPI)

Who you are.

**Q67.** What do ACLs control?

**Answer:** Which principals can read/write which resources

Authorization.

**Q68.** What's least privilege here?

**Answer:** Grant produce/consume on only the needed topics and groups

Scoped ACLs.

### [08 · Partition Reassignment &amp; Scaling a Cluster](level-2/08-reassignment-scaling.md)

**Q69.** What does partition reassignment do?

**Answer:** Moves replicas between brokers, e.g. after adding brokers

kafka-reassign-partitions.

**Q70.** Why throttle reassignments?

**Answer:** To protect production traffic from replication load

Bandwidth limits.

**Q71.** Can you reduce a topic's partition count?

**Answer:** No: only increase (which changes key mapping)

Plan upfront.

**Q72.** What happens to key ordering when adding partitions?

**Answer:** Key-to-partition mapping changes for new messages

Careful.

### [09 · Quotas &amp; Multi-Tenancy](level-2/09-quotas-multi-tenancy.md)

**Q73.** What do quotas protect?

**Answer:** The cluster from a noisy client or tenant

Produce/fetch/request quotas.

**Q74.** How are quotas identified?

**Answer:** By user and/or client.id

Per principal.

**Q75.** What happens when a client exceeds its quota?

**Answer:** The broker throttles it (delays responses)

Backoff.

**Q76.** What multi-tenancy practice helps?

**Answer:** Topic naming conventions, ACLs and quotas per team

Isolation.

### [10 · Project: Diagnose a Consumer Lag Problem](level-2/10-project-consumer-lag.md)

**Q77.** Lag is the difference between end offset and committed offset. Partitions end at [120, 90, 50] and committed [100, 90, 10]. Total lag?

```python
print(sum(e-c for e,c in zip([120,90,50],[100,90,10])))
```

**Answer:** 60

Sum across partitions. Verified by running it.

**Q78.** Why track lag per partition?

**Answer:** One slow partition can hide behind a healthy average

Skew.

**Q79.** What do you alert on?

**Answer:** Lag growing over time, not just a static number

Trend.

**Q80.** What reduces lag?

**Answer:** More consumers (up to partitions), faster processing or batching

Scale and tune.

## Level 3 · Kafka Connect & Kafka Streams

### [01 · Kafka Connect Architecture: Source &amp; Sink Connectors](level-3/01-connect-architecture.md)

**Q81.** What is Kafka Connect?

**Answer:** A framework for moving data between Kafka and external systems with connectors

No custom code.

**Q82.** What's a source vs sink connector?

**Answer:** Source ingests into Kafka; sink writes out of Kafka

Directions.

**Q83.** What does a converter do?

**Answer:** Serializes/deserializes data (JSON, Avro, Protobuf)

Format.

**Q84.** What are SMTs?

**Answer:** Single Message Transforms applied per record

Light changes.

### [02 · Running Connect Standalone](level-3/02-connect-standalone.md)

**Q85.** When use standalone Connect?

**Answer:** Development and simple single-process jobs

Offsets in a file.

**Q86.** Where does standalone store offsets?

**Answer:** In a local file

Not fault tolerant.

**Q87.** How configure a standalone connector?

**Answer:** Properties files passed at startup

Simple.

**Q88.** What limits standalone?

**Answer:** Single point of failure and no scaling

Use distributed.

### [03 · Running Connect Distributed &amp; the REST API](level-3/03-connect-distributed-rest.md)

**Q89.** What does distributed Connect provide?

**Answer:** Scalable, fault-tolerant workers coordinating via Kafka topics

Config/offset/status topics.

**Q90.** How manage connectors in distributed mode?

**Answer:** The REST API

POST /connectors.

**Q91.** What are tasks?

**Answer:** Units of parallel work a connector spawns

tasks.max.

**Q92.** What happens when a worker dies?

**Answer:** Tasks rebalance to remaining workers

Resilience.

### [04 · Kafka Streams Fundamentals: KStream vs KTable](level-3/04-streams-fundamentals.md)

**Q93.** What is Kafka Streams?

**Answer:** A client library for stateful stream processing in your app

No separate cluster.

**Q94.** KStream vs KTable?

**Answer:** KStream is an event stream; KTable is the latest value per key

Duality.

**Q95.** How does Streams scale?

**Answer:** Running more instances, each handling partitions (tasks)

Same app id.

**Q96.** What defines parallelism limits?

**Answer:** Input topic partition count

Tasks = partitions.

### [05 · Streams Stateful Operations &amp; State Stores](level-3/05-streams-state-stores.md)

**Q97.** What is a state store?

**Answer:** Local (RocksDB) storage for aggregations and joins

Backed by changelog.

**Q98.** What backs a state store for recovery?

**Answer:** A compacted changelog topic

Rebuild.

**Q99.** What speeds failover of state?

**Answer:** Standby replicas

num.standby.replicas.

**Q100.** Why is state local?

**Answer:** Fast access without network round trips

Co-partitioning.

### [06 · Streams Joins &amp; Windowing](level-3/06-streams-joins-windowing.md)

**Q101.** What must be true to join two streams/tables by key?

**Answer:** They are co-partitioned (same key and partition count)

Repartition if not.

**Q102.** What do windows do?

**Answer:** Group events by time (tumbling, hopping, sliding, session)

Time aggregates.

**Q103.** What is grace period for?

**Answer:** Accepting late events after a window closes

Late data.

**Q104.** What is a GlobalKTable?

**Answer:** A fully replicated table on every instance for lookups

Small reference data.

### [07 · Changelog Topics &amp; Fault Tolerance](level-3/07-changelog-fault-tolerance.md)

**Q105.** How does Streams recover state after a crash?

**Answer:** Replays the changelog topic to rebuild the store

Fault tolerance.

**Q106.** What does processing.guarantee=exactly_once_v2 provide?

**Answer:** Atomic consume-process-produce within Kafka

Transactions.

**Q107.** What causes long recovery?

**Answer:** Large state restored from changelog (use standbys)

Plan for it.

**Q108.** What compacts changelogs?

**Answer:** Log compaction keeps latest per key

Bounded size.

### [08 · Schema Evolution: Avro/Protobuf &amp; Compatibility](level-3/08-schema-evolution.md)

**Q109.** What does a schema registry provide?

**Answer:** Versioned schemas and compatibility checks for messages

Contracts.

**Q110.** Which change is backward compatible in Avro?

**Answer:** Adding a field with a default

Consumers keep working.

**Q111.** What does BACKWARD compatibility mean?

**Answer:** New schema can read data written with the old schema

Upgrade consumers first.

**Q112.** Why not free-form JSON?

**Answer:** No enforced contract, so breaking changes slip through

Use schemas.

### [09 · Error Handling: Dead Letter Queues &amp; Retries](level-3/09-error-handling-dlq.md)

**Q113.** What is a dead letter queue/topic?

**Answer:** A topic for records that repeatedly fail processing

Inspect later.

**Q114.** What should you store with a DLQ record?

**Answer:** Error details, source topic/partition/offset

Debuggability.

**Q115.** What's a poison pill?

**Answer:** A record that always fails processing and blocks progress

Isolate it.

**Q116.** How do Connect sinks handle errors?

**Answer:** errors.tolerance and deadletterqueue settings

Configurable.

### [10 · Project: A Real Kafka Streams Application](level-3/10-project-streams-application.md)

**Q117.** What does the Streams app project combine?

**Answer:** Topology, state, windows, joins and error handling

Level 3 project.

**Q118.** How test a topology without a cluster?

**Answer:** TopologyTestDriver

Fast tests.

**Q119.** How scale it?

**Answer:** Add instances up to the partition count

Tasks.

**Q120.** How monitor it?

**Answer:** Lag, state restore time and process rate

Metrics.

## Level 4 · Production Kafka & Capstone

### [01 · Performance Tuning: Throughput vs Latency](level-4/01-performance-tuning.md)

**Q121.** What most affects producer throughput?

**Answer:** Batching (linger.ms/batch.size), compression and acks

Tune together.

**Q122.** What affects consumer throughput?

**Answer:** fetch sizes, partitions and processing speed

Parallelism.

**Q123.** What hardware matters most for brokers?

**Answer:** Disk throughput/page cache and network

Sequential IO.

**Q124.** How tune safely?

**Answer:** Change one thing and measure with realistic load

Evidence.

### [02 · Capacity Planning: Partitions, Replication &amp; Disk](level-4/02-capacity-planning.md)

**Q125.** Ingest 50 MB/s with replication factor 3 and 7-day retention: roughly how many TB of disk across the cluster?

```python
print(round(50e6*86400*7*3/1e12,1))
```

**Answer:** 90.7

throughput x retention x replication. Verified by running it.

**Q126.** What drives partition count?

**Answer:** Target throughput and consumer parallelism

Plan upfront.

**Q127.** Why leave headroom?

**Answer:** Failures and rebalances shift load onto remaining brokers

N+1 capacity.

**Q128.** What network cost multiplies traffic?

**Answer:** Replication and fan-out to consumer groups

Count both.

### [03 · Troubleshooting: Under-Replicated Partitions &amp; ISR Shrink](level-4/03-troubleshooting-common-failures.md)

**Q129.** First step when consumers lag?

**Answer:** Check whether it's slow processing, a rebalance storm or broker issues

Diagnose.

**Q130.** What does repeated rebalancing suggest?

**Answer:** Too-slow polls, unstable members or short timeouts

Tune max.poll.

**Q131.** What does an offline partition imply?

**Answer:** No leader available: check ISR and broker health

Unclean election risks.

**Q132.** What is unclean leader election?

**Answer:** Electing an out-of-sync replica, risking data loss

Default off.

### [04 · Disaster Recovery &amp; Geo-Replication with MirrorMaker 2](level-4/04-disaster-recovery-mirrormaker.md)

**Q133.** What is MirrorMaker 2 for?

**Answer:** Replicating topics and offsets across clusters (DR/migration)

Cross-cluster.

**Q134.** What's active-passive DR?

**Answer:** One cluster serves traffic; the other is a replica ready to take over

Simpler.

**Q135.** What must consumers handle on failover?

**Answer:** Translated offsets and possible duplicates

Idempotent.

**Q136.** What defines acceptable DR?

**Answer:** RPO and RTO

Business targets.

### [05 · Upgrades, Compatibility &amp; the ZooKeeper-to-KRaft Migration](level-4/05-upgrades-kraft-migration.md)

**Q137.** How upgrade brokers safely?

**Answer:** Rolling restarts, one broker at a time, following compatibility notes

Zero downtime.

**Q138.** What's the order for protocol/metadata versions?

**Answer:** Upgrade binaries first, then bump metadata/feature versions

Rollback safety.

**Q139.** What does ZooKeeper-to-KRaft migration involve?

**Answer:** Dual-write migration mode with controllers before cutover

Follow the procedure.

**Q140.** Why test upgrades?

**Answer:** In staging with production-like load

Reduce risk.

### [06 · Kafka vs Other Messaging Systems](level-4/06-kafka-vs-other-systems.md)

**Q141.** When choose Kafka over a simple queue?

**Answer:** High-throughput streaming, replay and many independent consumers

Fit.

**Q142.** How does RabbitMQ differ?

**Answer:** Smart broker routing with message deletion on ack

Different model.

**Q143.** What do managed streams (Kinesis, Pub/Sub) trade?

**Answer:** Less ops for less control and portability

Choose by needs.

**Q144.** When is a database enough?

**Answer:** Low volume, simple needs with transactions

Don't over-engineer.

### [07 · Event-Driven Patterns: Event Sourcing, CQRS &amp; the Outbox](level-4/07-event-driven-patterns.md)

**Q145.** What is event sourcing?

**Answer:** Storing state changes as an immutable event log

Replay to rebuild.

**Q146.** What does CQRS separate?

**Answer:** Write and read models

Projections.

**Q147.** What's the outbox pattern?

**Answer:** Writing events with DB changes in one transaction then publishing them

Consistency.

**Q148.** What's a saga?

**Answer:** A sequence of local transactions with compensations

Distributed workflows.

### [08 · Kafka on Kubernetes with Strimzi](level-4/08-kafka-on-kubernetes.md)

**Q149.** What does Strimzi do?

**Answer:** Runs Kafka on Kubernetes via operators

Declarative.

**Q150.** What storage suits brokers on K8s?

**Answer:** Persistent volumes with fast disks

Durability.

**Q151.** What's key for K8s networking?

**Answer:** Stable broker addressing and advertised listeners

Client access.

**Q152.** What do pod disruption budgets protect?

**Answer:** Availability during node maintenance

Keep ISR.

### [09 · Cost &amp; Operational Tradeoffs at Scale](level-4/09-cost-operational-tradeoffs.md)

**Q153.** What drives Kafka cost?

**Answer:** Storage, network egress, broker count and ops effort

Retention and replication.

**Q154.** How cut storage cost?

**Answer:** Shorter retention, compression and tiered storage

Tier cold data.

**Q155.** What's tiered storage?

**Answer:** Offloading old segments to object storage

Cheaper retention.

**Q156.** Managed vs self-managed trade-off?

**Answer:** Convenience and SLAs versus control and cost at scale

Decide by team.

### [10 · Capstone: Build, Break &amp; Diagnose a Small Event-Driven Pipeline](level-4/10-capstone-event-driven-pipeline.md)

**Q157.** What does the capstone combine?

**Answer:** Producers, Connect, Streams, schemas, monitoring and DR in one pipeline

Event-driven pipeline.

**Q158.** How prove readiness?

**Answer:** Failure drills, load tests, lag alerts and runbooks

Operate.

**Q159.** What documentation matters?

**Answer:** Topic catalog, schemas, ownership and runbooks

Governance.

**Q160.** What sustains the platform?

**Answer:** Ownership, standards and capacity reviews

Lifecycle.

## Mixed questions across all levels

**Q161.** Why does Kafka guarantee ordering only within a partition?

**Answer:** Each partition is an independent append-only log; there is no global order across partitions

Use a key to keep related events together.

**Q162.** Which consumers are idle when 5 consumers share a 3-partition topic?

```python
print(5-3)
```

**Answer:** 2

Parallelism is bounded by partitions. Verified by running it.

**Q163.** Partition ends are [30, 10, 5] and committed offsets are [25, 10, 0]. Total lag?

```python
print(sum(e-c for e,c in zip([30,10,5],[25,10,0])))
```

**Answer:** 10

Lag = end offset minus committed offset. Verified by running it.

**Q164.** A producer retries after a lost ack and duplicates appear. What fixes it?

**Answer:** enable.idempotence=true (sequence numbers dedupe within a partition)

Safe retries.

**Q165.** How do you avoid data loss when a broker dies?

**Answer:** Replication factor 3, acks=all and min.insync.replicas=2

Durability settings.

**Q166.** Consumers commit offsets before processing and lose messages on a crash. What's the fix?

**Answer:** Commit after processing (at-least-once) and make handlers idempotent

Order of commit matters.

**Q167.** A consumer is repeatedly removed from its group. What do you check?

**Answer:** Poll interval/processing time (max.poll.interval.ms), session timeout and heartbeats

Rebalance storms.

**Q168.** Which topic setting keeps only the latest value per key?

**Answer:** cleanup.policy=compact

Changelogs and tables.

**Q169.** How do you delete a key from a compacted topic?

**Answer:** Produce a tombstone (null value)

Compaction removes it later.

**Q170.** An exactly-once pipeline writes to an external database. Does Kafka transactions cover it?

**Answer:** No: use an idempotent or transactional sink; Kafka's exactly-once covers Kafka-to-Kafka

Idempotent consumers.

**Q171.** What must consumers set to skip aborted transactional records?

**Answer:** isolation.level=read_committed

Transaction visibility.

**Q172.** Two streams must be joined by key but fail with an error. What's usually required?

**Answer:** Co-partitioning: same keys and the same partition count (repartition if needed)

Join prerequisite.

**Q173.** A Kafka Streams instance restarts and takes ages to serve. What reduces this?

**Answer:** Standby replicas and smaller state; state is rebuilt from the changelog

Fast failover.

**Q174.** A schema change breaks consumers. What prevents it?

**Answer:** A schema registry with compatibility checks (e.g. adding fields with defaults)

Contracts.

**Q175.** A poison message blocks a consumer. What's the pattern?

**Answer:** Retry a bounded number of times, then send to a dead-letter topic with error context

Keep the stream moving.

**Q176.** You need cross-region disaster recovery. Which tool?

**Answer:** MirrorMaker 2 (replicating topics and translating offsets)

Plan RPO/RTO.

**Q177.** Roughly how many TB of disk for 20 MB/s, RF 3, 3-day retention?

```python
print(round(20e6*86400*3*3/1e12,1))
```

**Answer:** 15.6

throughput x time x replication. Verified by running it.

**Q178.** Producers overload the cluster for other teams. What's the control?

**Answer:** Per-client/user quotas (produce/fetch/request)

Multi-tenancy.

**Q179.** What's the safe way to upgrade brokers?

**Answer:** Rolling restarts one at a time, upgrade binaries before bumping metadata/protocol versions

Zero downtime.

## Practical coding questions

### Partitions, consumer groups and lag

Model a topic with key-based partitioning and a consumer group: assignment() gives each partition to exactly one member (round-robin), poll() reads from the committed offset, commit() advances offsets and lag() reports end minus committed per partition. Check the rebalance with more members than partitions.

Starter code:

```
class Partition:
    def __init__(self):
        self.log = []                              # list of (key, value); offset = index

class Topic:
    def __init__(self, partitions):
        self.parts = [Partition() for _ in range(partitions)]

    def produce(self, key, value):
        p = sum(key.encode()) % len(self.parts)    # stable key -> partition (toy hash)
        self.parts[p].log.append((key, value))
        return p, len(self.parts[p].log) - 1

class Group:
    """A consumer group: each partition is owned by exactly one member; offsets are committed per partition."""
    def __init__(self, topic):
        self.topic = topic
        self.members = []
        self.committed = {i: 0 for i in range(len(topic.parts))}

    def assignment(self):
        # {member: [partition ids]} - each partition owned by exactly one member (round-robin by partition id)
        return {}

    def poll(self, member, max_records=10):
        # records for the member's partitions starting at the committed offset: [(partition, offset, (key, value))]
        return []

    def commit(self, records):
        # advance each partition's committed offset past the highest offset seen
        pass

    def lag(self):
        # {partition: end offset - committed offset}
        return {}

t = Topic(3)
for k in ["ada", "bo", "cy", "ada", "bo", "ada", "dee"]:
    t.produce(k, "event-" + k)
g = Group(t)
g.members = ["c1", "c2"]
print("assignment:", g.assignment())
print("lag:", g.lag())
r1 = g.poll("c1")
print("c1 got:", [(i, o) for i, o, _ in r1])
g.commit(r1)
print("lag after c1 commit:", g.lag())
g.members = ["c1", "c2", "c3", "c4"]                    # rebalance: more members than partitions
print("assignment:", g.assignment())
print("same key -> same partition:", len({t.produce("ada", "x")[0] for _ in range(5)}) == 1)
```

Expected output:

```text
assignment: {'c1': [0, 2], 'c2': [1]}
lag: {0: 3, 1: 1, 2: 3}
c1 got: [(0, 0), (0, 1), (0, 2), (2, 0), (2, 1), (2, 2)]
lag after c1 commit: {0: 0, 1: 1, 2: 0}
assignment: {'c1': [0], 'c2': [1], 'c3': [2], 'c4': []}
same key -> same partition: True
```

??? success "Show a solution"

    ```
    class Partition:
        def __init__(self):
            self.log = []                              # list of (key, value); offset = index

    class Topic:
        def __init__(self, partitions):
            self.parts = [Partition() for _ in range(partitions)]

        def produce(self, key, value):
            p = sum(key.encode()) % len(self.parts)    # stable key -> partition (toy hash)
            self.parts[p].log.append((key, value))
            return p, len(self.parts[p].log) - 1

    class Group:
        """A consumer group: each partition is owned by exactly one member; offsets are committed per partition."""
        def __init__(self, topic):
            self.topic = topic
            self.members = []
            self.committed = {i: 0 for i in range(len(topic.parts))}

        def assignment(self):
            out = {m: [] for m in self.members}
            for i in range(len(self.topic.parts)):
                out[self.members[i % len(self.members)]].append(i)       # round-robin
            return out

        def poll(self, member, max_records=10):
            got = []
            for i in self.assignment()[member]:
                log = self.topic.parts[i].log
                for off in range(self.committed[i], min(len(log), self.committed[i] + max_records)):
                    got.append((i, off, log[off]))
            return got

        def commit(self, records):
            for i, off, _ in records:
                self.committed[i] = max(self.committed[i], off + 1)

        def lag(self):
            return {i: len(self.topic.parts[i].log) - self.committed[i] for i in self.committed}

    t = Topic(3)
    for k in ["ada", "bo", "cy", "ada", "bo", "ada", "dee"]:
        t.produce(k, "event-" + k)
    g = Group(t)
    g.members = ["c1", "c2"]
    print("assignment:", g.assignment())
    print("lag:", g.lag())
    r1 = g.poll("c1")
    print("c1 got:", [(i, o) for i, o, _ in r1])
    g.commit(r1)
    print("lag after c1 commit:", g.lag())
    g.members = ["c1", "c2", "c3", "c4"]                    # rebalance: more members than partitions
    print("assignment:", g.assignment())
    print("same key -> same partition:", len({t.produce("ada", "x")[0] for _ in range(5)}) == 1)
    ```

    Each partition is owned by one member of the group, offsets are committed per partition and lag is end offset minus committed offset; extra members get no partitions. This is a teaching model, not the Kafka client. Verified by running the solution.

### Idempotent producer and at-least-once consumer

Complete Broker.append (dedupe by (producer, sequence) when enabled) and Consumer.run (process then commit; a crash before commit means the record is processed again). Compare plain retries with idempotence, and a naive consumer with an idempotent handler.

Starter code:

```
class Producer:
    """Idempotent producer model: retries after a lost ack must not create duplicates."""
    def __init__(self, broker):
        self.broker = broker
        self.seq = 0

    def send(self, value, fail_ack_times=0):
        seq = self.seq
        self.seq += 1
        attempts = 0
        while True:
            attempts += 1
            ack = self.broker.append(("p1", seq), value)
            if attempts > fail_ack_times:             # the first N acks are "lost" on the network
                return attempts

class Broker:
    def __init__(self, dedupe):
        self.log, self.seen, self.dedupe = [], set(), dedupe

    def append(self, pid_seq, value):
        # store value; when self.dedupe is on, a (producer, sequence) pair already seen must not be
        # stored again but should still be acknowledged. Always return True.
        return True

class Consumer:
    """At-least-once consumer: commits after processing; a crash before the commit means reprocessing."""
    def __init__(self, log, handler):
        self.log, self.handler, self.committed = log, handler, 0

    def run(self, crash_after=None):
        # process records from the committed offset, committing AFTER each is handled; when crash_after
        # records have been handled in this call, return "crashed" before committing that last one;
        # otherwise return "done" at the end of the log
        return "done"

for dedupe in (False, True):
    b = Broker(dedupe); p = Producer(b)
    p.send("a"); p.send("b", fail_ack_times=2); p.send("c")
    print("idempotence" if dedupe else "plain retries", "->", b.log)

b = Broker(True); p = Producer(b)
for v in "abc": p.send(v)
seen = []
c = Consumer(b.log, seen.append)
print(c.run(crash_after=2), seen, "committed", c.committed)
print(c.run(), seen, "committed", c.committed)
effects = {}
def idempotent(v): effects[v] = effects.get(v, 0) + 1
c2 = Consumer(b.log, lambda v: idempotent(v)); c2.run(crash_after=2); c2.run()
print("side effects by key (naive):", effects)
done = set(); applied = []
def dedup_handler(v):
    if v not in done:
        done.add(v); applied.append(v)
c3 = Consumer(b.log, dedup_handler); c3.run(crash_after=2); c3.run()
print("applied with idempotent handler:", applied)
```

Expected output:

```text
plain retries -> ['a', 'b', 'b', 'b', 'c']
idempotence -> ['a', 'b', 'c']
crashed ['a', 'b'] committed 1
done ['a', 'b', 'b', 'c'] committed 3
side effects by key (naive): {'a': 1, 'b': 2, 'c': 1}
applied with idempotent handler: ['a', 'b', 'c']
```

??? success "Show a solution"

    ```
    class Producer:
        """Idempotent producer model: retries after a lost ack must not create duplicates."""
        def __init__(self, broker):
            self.broker = broker
            self.seq = 0

        def send(self, value, fail_ack_times=0):
            seq = self.seq
            self.seq += 1
            attempts = 0
            while True:
                attempts += 1
                ack = self.broker.append(("p1", seq), value)
                if attempts > fail_ack_times:             # the first N acks are "lost" on the network
                    return attempts

    class Broker:
        def __init__(self, dedupe):
            self.log, self.seen, self.dedupe = [], set(), dedupe

        def append(self, pid_seq, value):
            if self.dedupe and pid_seq in self.seen:
                return True                                # duplicate detected, already stored
            self.seen.add(pid_seq)
            self.log.append(value)
            return True

    class Consumer:
        """At-least-once consumer: commits after processing; a crash before the commit means reprocessing."""
        def __init__(self, log, handler):
            self.log, self.handler, self.committed = log, handler, 0

        def run(self, crash_after=None):
            processed = 0
            while self.committed < len(self.log):
                self.handler(self.log[self.committed])
                processed += 1
                if crash_after is not None and processed == crash_after:
                    return "crashed"                       # processed but not yet committed
                self.committed += 1
            return "done"

    for dedupe in (False, True):
        b = Broker(dedupe); p = Producer(b)
        p.send("a"); p.send("b", fail_ack_times=2); p.send("c")
        print("idempotence" if dedupe else "plain retries", "->", b.log)

    b = Broker(True); p = Producer(b)
    for v in "abc": p.send(v)
    seen = []
    c = Consumer(b.log, seen.append)
    print(c.run(crash_after=2), seen, "committed", c.committed)
    print(c.run(), seen, "committed", c.committed)
    effects = {}
    def idempotent(v): effects[v] = effects.get(v, 0) + 1
    c2 = Consumer(b.log, lambda v: idempotent(v)); c2.run(crash_after=2); c2.run()
    print("side effects by key (naive):", effects)
    done = set(); applied = []
    def dedup_handler(v):
        if v not in done:
            done.add(v); applied.append(v)
    c3 = Consumer(b.log, dedup_handler); c3.run(crash_after=2); c3.run()
    print("applied with idempotent handler:", applied)
    ```

    Sequence numbers let the broker drop duplicate retries, while commit-after-processing gives at-least-once delivery, so side effects must be idempotent. Verified by running the solution.

---

More free practice: the [Kafka Mastery Path](index.md) curriculum, the [Interview Prep Pack](https://learnmasterypath.com/interview-pack/) and the [free resume ATS check](https://learnmasterypath.com/resume/).
