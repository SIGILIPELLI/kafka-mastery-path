---
description: "Leaders, followers and the ISR demonstrated on a real 3-broker cluster: a real leader failover, zero data loss, and a replica rejoining."
---

# 07 · Replication: Leaders, Followers & the ISR

Everything in this course up to now ran on a single broker with replication factor 1 — fine for learning the
log model, but it can't show the one property that makes Kafka trustworthy for real data: surviving a broker
failure without losing anything that was already acknowledged. This lesson runs a real 3-broker cluster and
actually kills one of them.

## Leaders, followers, and the in-sync replica set

Each partition has exactly one **leader** broker, which handles all produce and fetch requests for it, and some
number of **followers**, which continuously replicate the leader's log. The **ISR** (in-sync replica set) is
the subset of replicas — leader included — that are fully caught up with the leader at any given moment. A
message is only considered **committed** once it's been written to every replica in the ISR (governed by the
producer's `acks` setting and the topic's `min.insync.replicas`, covered in depth in Level 2) — which is exactly
what makes committed data survive the loss of any one broker in that ISR.

## Worked example: a real 3-broker cluster, a real kill, a real failover

```text
$ bin/kafka-topics.sh --create --topic repl-topic --bootstrap-server localhost:9092 \
    --partitions 1 --replication-factor 3
$ bin/kafka-topics.sh --describe --topic repl-topic --bootstrap-server localhost:9092
Topic: repl-topic  Partition: 0  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2

$ echo -e "msg1\nmsg2\nmsg3" | bin/kafka-console-producer.sh --topic repl-topic --bootstrap-server localhost:9092

$ kill <broker-3-pid>          # the current leader, killed outright

$ bin/kafka-topics.sh --describe --topic repl-topic --bootstrap-server localhost:9092,localhost:9094
Topic: repl-topic  Partition: 0  Leader: 1  Replicas: 3,1,2  Isr: 1,2

$ bin/kafka-console-consumer.sh --topic repl-topic --from-beginning --bootstrap-server localhost:9092 --max-messages 3
msg1
msg2
msg3

$ # restart broker 3
$ bin/kafka-topics.sh --describe --topic repl-topic --bootstrap-server localhost:9092
Topic: repl-topic  Partition: 0  Leader: 1  Replicas: 3,1,2  Isr: 1,2,3
```

(Real output from this course's lab — a genuine 3-node KRaft cluster, a genuinely killed broker process, not a
simulation.) Before the kill: broker 3 leads, and all three replicas (`3,1,2`) are in the ISR. After killing
broker 3 outright: a **new leader (broker 1)** was elected automatically within seconds, and the `Isr` shrank to
`1,2` — broker 3 dropped out because it's no longer reachable — while `Replicas` stayed `3,1,2`, because the
*assigned* replica set doesn't change just because one member is temporarily down; only the ISR, which tracks
who's actually caught up *right now*, shrinks. Critically, **all three messages produced before the kill were
still readable afterward** — nothing was lost, because they'd already been replicated to the surviving brokers
before broker 3 went down. Restarting broker 3 brought it back into the ISR (`1,2,3`) as a follower, without
taking back leadership — Kafka doesn't force leadership back to a recovering replica automatically by default.

## How It Actually Works

Replication works by followers continuously **fetching** from the leader, the same way a consumer fetches — a
follower is, mechanically, a special kind of consumer of its own leader's log. The leader tracks each follower's
fetch progress, and a replica only stays in the ISR as long as it's fetching recently enough (governed by
`replica.lag.time.max.ms`); fall behind that threshold, or stop responding entirely (as this lesson's killed
broker did), and the leader drops it from the ISR, shrinking the set of replicas a commit has to reach before
being acknowledged. This shrink is itself informative to monitor in production: a shrinking ISR under normal
operation (not a deliberate kill) is one of the most common early signals of real trouble — a struggling disk,
network partition, or overloaded broker falling behind on replication.

Leader election, when a leader fails, is itself coordinated through the **KRaft controller quorum** (Lesson 2)
rather than through the data-plane brokers negotiating among themselves — the controller tracks replica state
for every partition cluster-wide and picks a new leader from the remaining in-sync replicas the instant it
detects the old leader is gone. This is exactly why replication and the controller quorum, though conceptually
separate (Lesson 2), depend on each other in practice: data-plane failover speed and correctness rest on the
controller quorum being healthy and responsive.

## Common mistakes

- **Running replication factor 1 in anything beyond local development "to save resources."** As this lesson
  demonstrates directly, a single-replica partition has zero tolerance for that one broker's loss — there's no
  ISR to fail over to at all.
- **Assuming a shrunk ISR self-heals as soon as the missing broker comes back.** As shown above, the replica
  does rejoin the ISR, but **not** the leadership position it previously held — conflating "back in the ISR"
  with "back to its old role" misreads what actually happened.
- **Confusing `Replicas` with `Isr` when reading `--describe` output.** `Replicas` is the assigned, intended
  set regardless of current health; `Isr` is who's actually caught up right now — a partition can show a full
  `Replicas` list while its `Isr` is dangerously shrunk, and that gap is exactly what's worth alerting on.

## Exercise

Reproduce this lesson's 3-broker setup, create a replication-factor-3 topic, and produce some data. Kill the
**current leader** broker and confirm via `--describe` that a new leader was elected and the ISR shrank.
Confirm no data was lost by consuming from the beginning. Then restart the killed broker and watch the ISR
heal — note which broker holds leadership afterward, and whether it matches your expectation.
