---
description: "Kafka Level 3: Kafka Connect architecture and the REST API, Kafka Streams topology, state stores, joins/windows, and schema evolution."
---

# Level 3 · Kafka Connect & Kafka Streams

Level 3 covers the two most common reasons teams reach for Kafka beyond raw producers and consumers: Connect for
moving data between Kafka and other systems without hand-written client code, and Streams for processing data as
it arrives rather than batching it later. Both are run for real in this level, including a genuine configuration
gotcha this course hit while building it.

!!! note "What was run, and what wasn't"
    Kafka Connect (standalone, with the bundled file source/sink connectors) and Kafka Streams (the bundled
    word-count example application) were both run end-to-end against this course's real broker — real topics,
    real consumer groups, real changelog topics, real state stores on disk. Connect's distributed mode and
    cluster-wide rebalancing across multiple worker processes is described from a single-worker run and the
    Kafka documentation, since multiple independent worker machines were outside this lab's scope; this is
    stated in that lesson specifically.

**Prerequisites:** Level 1 and 2. Basic Java familiarity helps for reading Streams code, though the DSL itself is
approachable without deep Java experience — see the [Java Mastery Path](https://sigilipelli.github.io/java-mastery-path/)
if you want it in depth first.

## Modules

1. **[Kafka Connect Architecture](01-connect-architecture.md)**: source and sink connectors, workers, and tasks.
2. **[Running Connect Standalone](02-connect-standalone.md)**: a real file-to-Kafka pipeline, and a real
   classpath gotcha this course hit along the way.
3. **[Running Connect Distributed & the REST API](03-connect-distributed-rest.md)**: managing connectors over
   HTTP instead of a config file.
4. **[Kafka Streams Fundamentals](04-streams-fundamentals.md)**: `KStream` vs `KTable`, and the topology that
   connects them.
5. **[Streams Stateful Operations & State Stores](05-streams-state-stores.md)**: aggregations, and the
   changelog topics that back them.
6. **[Streams Joins & Windowing](06-streams-joins-windowing.md)**: combining streams, and time-bounded
   aggregation.
7. **[Changelog Topics & Fault Tolerance](07-changelog-fault-tolerance.md)**: how a Streams app recovers its
   state after a crash.
8. **[Schema Evolution](08-schema-evolution.md)**: Avro/Protobuf, compatibility modes, and what breaks a
   consumer.
9. **[Error Handling: Dead Letter Queues & Retries](09-error-handling-dlq.md)**: what to do with a message your
   processing logic can't handle.
10. **[Project: A Real Kafka Streams Application](10-project-streams-application.md)**: build and run a stateful
    streaming aggregation end to end.

## After Level 3

Level 4 is production concerns: performance tuning, capacity planning, real failure modes, disaster recovery,
event-driven architecture patterns, and a capstone that builds, breaks, and diagnoses a small pipeline.
