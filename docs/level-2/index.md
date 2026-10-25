---
description: "Kafka Level 2: producer/consumer tuning, exactly-once transactions, real metrics, log internals, security, reassignment and quotas."
---

# Level 2 · Operating a Cluster

Level 1 showed Kafka working. Level 2 is about knowing why a specific configuration value matters, what a metric
is actually telling you, and how to change a running cluster's shape without losing data. Every configuration
change and every metric in this level was observed on a real local broker, not quoted from documentation alone.

!!! note "What was run, and what wasn't"
    Producer/consumer configuration, exactly-once transactions, JMX metrics, log segment behavior, and partition
    reassignment were all exercised on this course's real single-broker lab. Multi-broker-specific behavior
    (reassignment actually moving data between distinct machines, quota enforcement across many tenants) is
    described from a single broker acting in all roles at once and says so; the mechanism shown is real even
    where the lab's one-broker scale can't fully reproduce a production cluster's scale.

**Prerequisites:** Level 1. Comfort reading JSON-ish configuration output helps for the metrics lesson.

## Modules

1. **[Multi-Broker Clusters & the KRaft Quorum in Practice](01-multi-broker-kraft-quorum.md)**: what changes
   when you add brokers, and how the KRaft controller quorum tolerates failure.
2. **[Producer Configuration That Actually Matters](02-producer-configuration.md)**: `acks`, `batch.size`,
   `linger.ms`, `compression.type`, measured, not just defined.
3. **[Consumer Configuration & Fetch Tuning](03-consumer-configuration.md)**: `fetch.min.bytes`,
   `max.poll.records`, session timeouts, and their real throughput/latency tradeoffs.
4. **[Exactly-Once Semantics](04-exactly-once-transactions.md)**: the idempotent producer and the transactional
   API, run for real.
5. **[Monitoring: The Metrics That Actually Matter](05-monitoring-metrics.md)**: consumer lag, under-replicated
   partitions, request latency — read from a real JMX endpoint.
6. **[Log Segments, Indexes & Cleanup in Practice](06-segments-indexes-cleanup.md)**: watching segments roll and
   old ones get deleted, on disk, in real time.
7. **[Security: SASL, SSL & ACLs](07-security-sasl-ssl-acls.md)**: authenticating and authorizing a real client
   against a real broker.
8. **[Partition Reassignment & Scaling a Cluster](08-reassignment-scaling.md)**: moving partitions without
   losing messages.
9. **[Quotas & Multi-Tenancy](09-quotas-multi-tenancy.md)**: throttling one client without starving the rest.
10. **[Project: Diagnose a Consumer Lag Problem](10-project-consumer-lag.md)**: a real, deliberately slow
    consumer, found and explained with real tools.

## After Level 2

Level 3 builds on top of the broker: Kafka Connect for moving data in and out without custom client code, and
Kafka Streams for processing it as it arrives.
