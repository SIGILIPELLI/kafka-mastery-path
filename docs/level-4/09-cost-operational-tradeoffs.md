---
description: "Self-managed vs managed Kafka cost tradeoffs, grounded in this course's own real measured throughput, storage overhead, and replication numbers."
---

# 09 · Cost & Operational Tradeoffs at Scale

"Self-managed is cheaper" and "managed is worth the premium" are both true, depending entirely on which real
numbers you plug in. This lesson uses the actual measured figures from earlier in this level — not invented
estimates — to make the self-managed-vs-managed decision concrete instead of abstract.

## Grounding the storage side in real numbers

Level 4, Lesson 2 measured, directly, that a replication-factor-3 topic consumes **roughly 3x** raw payload
volume in real disk, plus a real, measured ~10 bytes/record structural overhead. Scaling that up: a workload
ingesting 100 GB/day of raw payload, at replication factor 3, with 7-day retention, needs roughly
`100 GB × 3 × 7 ≈ 2.1 TB` of steady-state cluster storage — a number derived directly from this course's own
measurements, not a vendor's marketing page. That 2.1 TB has to live *somewhere*: attached disk you provision
and pay for directly on self-managed infrastructure, or storage cost bundled into a managed service's per-GB
or per-partition-hour pricing.

## Grounding the throughput side in real numbers

Level 4, Lesson 1 measured real producer throughput varying from roughly 41,000 to 88,000 records/sec
depending purely on compression codec choice, on one laptop's worth of CPU. That's the real, concrete shape of
the tradeoff self-managed infrastructure gives you direct control over: you can choose `none` for raw speed,
accept `gzip`'s throughput cost for smaller storage and network bills, or benchmark your own workload (as
Lesson 1's own surprising lz4-latency result showed is necessary) rather than trusting a number from
documentation. A managed service abstracts this choice into a pricing tier, but doesn't remove the underlying
tradeoff — your actual bytes still have to be compressed, stored, and replicated somewhere, by someone's CPU
and disk.

## How It Actually Works

**Self-managed** Kafka's real cost structure is: infrastructure (the actual compute/disk this course's lab
used, scaled up) plus operational labor — the actual, nontrivial work this entire course demonstrated by
hand: formatting and configuring brokers (Level 1, Lesson 2), diagnosing a real broker failure (Level 1,
Lesson 7; Level 4, Lesson 3), setting up security (Level 2, Lesson 7), running reassignments (Level 2, Lesson
8), and so on. Every one of those hands-on lessons represents real engineering time a self-managed deployment
needs *someone* to actually do, repeatedly, as the cluster grows and changes — that labor cost is real, even
though it doesn't show up on an infrastructure invoice the way a managed service's bill does.

**Managed Kafka** (Confluent Cloud, Amazon MSK, and similar) prices in that operational labor as part of the
per-unit cost — you're paying, in effect, for someone else to have already built the automation this course's
Level 4, Lesson 8 described Strimzi doing, plus a team on call for exactly the failure scenarios Lesson 3's
real broker-kill test demonstrated. The crossover point — where self-managed's lower raw infrastructure cost
stops winning against managed's lower labor cost — depends heavily on how much of this course's hands-on
material (replication tuning, security configuration, failure diagnosis, capacity planning) your team would
otherwise need to learn and maintain expertise in internally, which is a real, if hard-to-put-a-single-number-
on, cost.

## Common mistakes

- **Comparing only the sticker price of infrastructure vs. a managed service's bill**, ignoring the real
  operational labor self-managed Kafka requires — every "how do I..." this course answered by hand is a real
  skill someone on a self-managed team needs to have, continuously, not just once at setup time.
- **Assuming managed Kafka removes the need to understand Kafka's own concepts.** As Lesson 8 covered for
  Kubernetes/Strimzi specifically, a managed service still requires understanding replication, partitioning,
  and consumer group behavior to use correctly and diagnose problems — it changes who operates the
  infrastructure, not what you need to know about the system itself.
- **Sizing a cost comparison off of throughput or storage estimates instead of a real benchmark.** As this
  level's own lz4-latency surprise (Lesson 1) and per-record overhead measurement (Lesson 2) showed directly,
  real numbers on your actual workload can differ meaningfully from generic estimates — and that difference
  translates directly into cost at scale.

## Exercise

Using this level's real measured numbers (storage overhead from Lesson 2, throughput from Lesson 1) as a
starting point, estimate the steady-state storage and the minimum broker capacity your own hypothetical
workload (pick a real or plausible daily ingest volume) would need. Then separately estimate, honestly, how
many hours per month a team would spend on the operational tasks this course demonstrated by hand (security
setup, failure diagnosis, reassignment, upgrades) — and decide, with both real numbers in hand, where your
own crossover point between self-managed and managed would actually sit.
