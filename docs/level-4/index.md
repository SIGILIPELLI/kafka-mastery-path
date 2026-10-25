---
description: "Kafka Level 4: performance tuning, capacity planning, real failure modes, disaster recovery, event-driven patterns, Kubernetes and a capstone."
---

# Level 4 · Production Kafka & Capstone

Level 4 is what you need once Kafka is actually running something that matters: how to reason about throughput
vs latency tradeoffs, plan capacity before you're paged about it, recognize and fix the handful of failure modes
that account for most real incidents, and the architectural patterns Kafka is usually the backbone of. It ends
with a capstone that builds, deliberately breaks, and diagnoses a small event-driven pipeline.

!!! note "What was run, and what wasn't"
    Performance measurements, failure-mode reproductions (a killed broker, a slow consumer, a too-small
    partition count), and the capstone's injected faults were run against this course's real single-broker lab —
    real numbers, not invented ones, with this lab's modest hardware stated plainly wherever a number is
    environment-sensitive. Multi-datacenter disaster recovery, Kubernetes operator behavior at real cluster
    scale, and cost figures for managed Kafka services are described from documentation and marked as such; this
    course doesn't have a second datacenter or a production Kubernetes cluster to run them against honestly.

**Prerequisites:** Levels 1-3. The capstone assumes comfort combining topics, producers/consumers, and basic
Streams concepts from every earlier level.

## Modules

1. **[Performance Tuning](01-performance-tuning.md)**: throughput vs latency, measured on a real producer.
2. **[Capacity Planning](02-capacity-planning.md)**: partitions, replication factor, and disk, sized from real
   numbers.
3. **[Troubleshooting Common Failures](03-troubleshooting-common-failures.md)**: under-replicated partitions
   and ISR shrink, reproduced for real.
4. **[Disaster Recovery & Geo-Replication with MirrorMaker 2](04-disaster-recovery-mirrormaker.md)**: mirroring
   topics between clusters.
5. **[Upgrades, Compatibility & the KRaft Migration](05-upgrades-kraft-migration.md)**: what changed when
   ZooKeeper was removed, and how rolling upgrades work.
6. **[Kafka vs Other Messaging Systems](06-kafka-vs-other-systems.md)**: RabbitMQ, Pulsar, and cloud queues,
   compared honestly.
7. **[Event-Driven Patterns](07-event-driven-patterns.md)**: event sourcing, CQRS, and the outbox pattern, with
   Kafka as the backbone.
8. **[Kafka on Kubernetes with Strimzi](08-kafka-on-kubernetes.md)**: what an operator actually automates.
9. **[Cost & Operational Tradeoffs at Scale](09-cost-operational-tradeoffs.md)**: self-managed vs managed Kafka,
   honestly weighed.
10. **[Capstone: Build, Break & Diagnose a Small Event-Driven Pipeline](10-capstone-event-driven-pipeline.md)**:
    three real, injected faults, diagnosed with the tools from every level.
