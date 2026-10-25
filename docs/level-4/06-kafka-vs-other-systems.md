---
description: "Kafka compared honestly against RabbitMQ, Apache Pulsar, and cloud queues (SQS/SNS) on the structural differences that actually matter for choosing one."
---

# 06 · Kafka vs Other Messaging Systems

!!! note "Documentation-based lesson"
    Unlike almost every other lesson in this course, this one is a comparison *across* systems this lab never
    installed — RabbitMQ, Apache Pulsar, and AWS SQS/SNS. The facts below are drawn from each project's own
    current documentation as of when this course was written, not from running any of them. Treat specific
    version details as a snapshot; the structural differences below are durable.

Every lesson in this course leaned on one fact: a Kafka topic is a durable, replayable, partitioned log. That
single design choice is also exactly what separates Kafka from the other major messaging systems — not "Kafka
is better," but "Kafka solved a different problem than these did first."

## Four systems, four different starting assumptions

**RabbitMQ** started from the classic message-queue model: a message is delivered, acknowledged, and then
**gone** — no replay, no "read from the beginning" the way Level 1's lessons took for granted. It offers rich,
flexible routing (exchanges, bindings, multiple routing patterns) that Kafka deliberately doesn't — Kafka's
"routing" is just "which topic/partition," full stop. RabbitMQ is the right default reach for classic
task-queue and RPC-style messaging patterns where replay isn't the point and per-message routing flexibility
is.

**Apache Pulsar** is architecturally the closest to Kafka's core promise (a durable, replayable log) but
makes a different foundational choice: Pulsar **separates compute (brokers) from storage (a dedicated
storage layer, historically Apache BookKeeper)**, whereas Kafka brokers own their own local disk directly.
This split lets Pulsar scale storage and serving independently and offers built-in multi-tenancy and
geo-replication as first-class platform features, at the cost of a more complex overall architecture with more
moving parts to operate than a Kafka cluster's comparatively simpler broker-owns-its-disk model.

**Cloud queues (AWS SQS/SNS, and equivalents on other clouds)** are fully-managed, pay-per-use services with
effectively no operational burden on your side at all — no brokers to run, no disks to size (Level 4, Lesson
2's whole exercise doesn't apply). The tradeoff is real constraints Kafka doesn't impose: SQS messages have
retention limits measured in days (not Kafka's effectively unbounded retention), no true partition-ordering
model the way Kafka guarantees per-key order, and per-request pricing that can make very high-throughput
streaming meaningfully more expensive than operating your own (or a managed) Kafka cluster at the same volume.

## How It Actually Works

The deepest, most consequential difference running through all of this course's Level 1 material is
**consumption model**: Kafka consumers *pull* and track their own offset (Level 1, Lesson 5) — the broker has
no idea which messages a consumer has "finished," only where that consumer's committed offset currently sits.
RabbitMQ and SQS use a **push-and-acknowledge** model instead: the broker hands a message to a consumer and
waits for an explicit ack (or redelivers it after a timeout if none arrives), tracking per-message delivery
state directly. This single difference explains almost everything else: Kafka's pull model is what makes
replay trivial (just reset your own offset) and what makes a single topic servable to many independent
consumer groups at wildly different processing speeds without the broker tracking anything per-consumer beyond
offsets (Level 1, Lesson 5's consumer group mechanics); the push-ack model is what makes RabbitMQ/SQS a more
natural fit for classic "exactly one worker should handle this task" queue semantics, without needing the
consumer-group partition-assignment machinery Kafka relies on for the same effect.

Pulsar's broker/storage split addresses a genuine operational pain point Kafka's design does have: in Kafka,
scaling storage and scaling serving capacity are coupled, because both live on the same broker process and
disk — adding capacity for one often means paying for the other whether you need it or not. Pulsar's
architecture decouples them, at the real cost of operating (or paying a managed service to operate) more
distinct components.

## Common mistakes

- **Choosing Kafka by default for every messaging need "because it's the industry standard."** A classic
  task queue with no replay requirement, modest throughput, and a need for flexible routing is often a
  genuinely better fit for RabbitMQ, with far less operational complexity than standing up and running Kafka
  well.
- **Underestimating cloud-queue retention and ordering limits for a use case that actually needs Kafka's
  guarantees.** A design assuming SQS behaves like a replayable, strictly-ordered-per-key log will hit real,
  documented limits that a quick proof-of-concept might not surface until scale or retention needs grow.
- **Assuming Pulsar's architectural elegance is free.** More separable components generally means more pieces
  to operate, monitor, and reason about during an incident — a real cost against Kafka's comparatively simpler
  (if less flexible) broker-owns-its-disk model.

## Exercise

For a messaging need you're familiar with (a real system you've worked on, or a hypothetical one), write one
paragraph each arguing for Kafka, RabbitMQ, and a cloud-managed queue as the right choice — forcing yourself to
find a genuine, specific argument for each, not just restating "Kafka is the standard," sharpens exactly the
kind of judgment this comparison is meant to build.
