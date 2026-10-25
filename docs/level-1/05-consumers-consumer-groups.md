---
description: "Consumer groups and offsets explained through a real 2-to-3-to-2 consumer rebalance, with real partition reassignment at every step."
---

# 05 · Consumers & Consumer Groups

A single consumer reading a topic is simple: it tracks its own offset and moves forward. A **consumer group** —
multiple consumers sharing the work of reading a topic — is where the interesting mechanics live, and this
lesson watches a real group grow from two consumers to three and back down to two, with real partition
reassignment at every step.

## The core rule

Every partition of a topic is assigned to **at most one consumer within a given group** at any time. This is
the entire mechanism behind "consumers in a group never double-read a partition" — it isn't a coordination
trick between consumers, it's a hard assignment the **group coordinator** (a role one broker plays for each
group) computes and hands out. A consumer tracks its position per partition as a **committed offset**, stored in
Kafka's own internal `__consumer_offsets` topic — so a consumer that restarts resumes from where it left off,
rather than from the beginning or from "latest."

## Worked example: a real group, growing and shrinking

```text
$ # two consumers join group "demo-group"
$ bin/kafka-consumer-groups.sh --describe --group demo-group --bootstrap-server localhost:9092
PARTITION  CONSUMER-ID
0          console-consumer-807b0133...
1          console-consumer-807b0133...
2          console-consumer-941413a0...

$ # a third consumer joins
$ bin/kafka-consumer-groups.sh --describe --group demo-group --bootstrap-server localhost:9092
PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID
1          3               3               0    console-consumer-941413a0...
0          9               9               0    console-consumer-807b0133...
2          2               2               0    console-consumer-9d68c153...

$ # kill the consumer holding partition 1 (941413a0)
$ bin/kafka-consumer-groups.sh --describe --group demo-group --bootstrap-server localhost:9092
PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID
0          9               9               0    console-consumer-807b0133...
1          3               3               0    console-consumer-807b0133...
2          2               2               0    console-consumer-9d68c153...
```

(Real output from this course's lab broker, three real console-consumer processes, started and killed in
sequence.) With **2 consumers and 3 partitions**: one consumer (`807b0133`) got two partitions, the other
(`941413a0`) got one — an uneven but valid split, since 3 doesn't divide evenly by 2. The instant a **third**
consumer joined, the group rebalanced to a clean one-partition-each split — notice `941413a0` moved from
partition 2 to partition 1, a real reassignment, not just a new consumer picking up what was unclaimed.
When that same consumer (`941413a0`) was killed, the group rebalanced again: partition 1 was picked back up by
`807b0133` (the surviving consumer that already held partition 0), restoring the uneven 2-and-1 split from the
start — and crucially, `LAG` stayed at `0` throughout, because the *committed offset* for each partition
persisted in `__consumer_offsets` regardless of which specific consumer process is currently assigned to read
from it.

## How It Actually Works

Offsets are committed to `__consumer_offsets`, a normal (if internal) Kafka topic, keyed by
`(group, topic, partition)`. This is why offset tracking survives individual consumer crashes and even entire
group membership changes: the offset isn't "remembered" by any one consumer process in memory, it's durably
stored in Kafka itself, and whichever consumer gets assigned a given partition next simply reads the last
committed offset for that `(group, topic, partition)` key and resumes from there. This is also exactly why
restarting a whole consumer group with the *same* group ID resumes where it left off, while using a **new**
group ID (or `--from-beginning` on a brand-new group) starts over from scratch — the committed offsets are
scoped to the group identity, not to the data itself.

The actual rebalance mechanics — *how* the coordinator decides which consumer gets which partitions, and
whether every consumer has to stop reading while it happens — have changed significantly over Kafka's history,
which is exactly what the next lesson covers in depth: the classic "stop-the-world" eager rebalance protocol,
the cooperative incremental protocol that followed it, and KIP-848's newer consumer group protocol (the one this
very lab's console consumer advertised as "production-ready" in its own startup banner).

## Common mistakes

- **Assuming more consumers in a group always means more parallelism.** A group can never usefully have more
  active consumers than the topic has partitions — any consumer beyond that count sits completely idle, since
  every partition is already claimed.
- **Confusing LAG with "the consumer is behind."** `LAG` is the gap between a partition's `LOG-END-OFFSET` and
  the group's `CURRENT-OFFSET` for it — a healthy, keeping-up consumer shows `LAG: 0` exactly like this lesson's
  example, and a consistently growing (not just momentarily nonzero) `LAG` is the real signal worth
  investigating (Level 2's consumer-lag project goes into this directly).
- **Using a brand new consumer group ID for a quick test and being surprised it reads from the very beginning
  or misses existing data depending on `auto.offset.reset`.** A group ID with no prior committed offsets falls
  back to this setting (`earliest` or `latest`) — know which your tooling defaults to before concluding data is
  "missing."

## Exercise

Reproduce this lesson's 2-to-3-to-2 consumer sequence yourself against a 3-partition topic, running
`kafka-consumer-groups.sh --describe` after each change. Then try the opposite experiment: use a topic with
**6** partitions and only 2 consumers, and confirm each consumer ends up with 3 partitions rather than one
sitting idle.
