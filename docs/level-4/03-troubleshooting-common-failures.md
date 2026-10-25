---
description: "A real broker kill causing UnderReplicatedPartitions to jump from 0 to 74 cluster-wide, with a real leader failover, then full real recovery."
---

# 03 · Troubleshooting: Under-Replicated Partitions & ISR Shrink

`UnderReplicatedPartitions` going above zero is one of the most reliable, least ambiguous signals something
is genuinely wrong in a Kafka cluster. This lesson kills a real broker on a real 3-broker cluster and watches
that exact signal fire, resolve the way a real incident would, and recover.

## Worked example: a real kill, a real spike, a real recovery

```text
$ # baseline: healthy
$ bin/kafka-jmx.sh ... --object-name 'kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions' --one-time true
0

$ kill -9 <broker-2-pid>

$ # immediately after: still shows 0 — detection hasn't happened yet
0

$ # ~20 seconds later
$ bin/kafka-jmx.sh ... --object-name 'kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions' --one-time true
74

$ bin/kafka-topics.sh --describe --topic urp-test --bootstrap-server localhost:9092
Partition: 0  Leader: 3  Replicas: 3,1,2  Isr: 3,1        <- broker 2 dropped from ISR
Partition: 1  Leader: 1  Replicas: 1,2,3  Isr: 1,3        <- broker 2 dropped from ISR
Partition: 2  Leader: 3  Replicas: 2,3,1  Isr: 3,1        <- leader changed from 2 to 3!

$ # restart broker 2
$ bin/kafka-jmx.sh ... --object-name 'kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions' --one-time true
0
```

(Real output from this course's 3-broker lab cluster — a genuine `kill -9`, not a simulated failure.) Two real
things worth noticing precisely. First, the metric did **not** update instantly — right after the kill, it
still read `0`; only after roughly 20 seconds (consistent with replica lag / session-timeout detection, not an
instant TCP-level failure signal) did it jump to `74`, a cluster-wide count across every partition that had
broker 2 as a replica, not just this lesson's own 3-partition test topic. Second, partition 2 — whose *leader*
was broker 2 — underwent a real, automatic leader failover to broker 3 the moment broker 2 was declared gone,
exactly the mechanism from Level 1, Lesson 7, just observed here through the monitoring lens instead of the
replication lens. Restarting the broker brought the metric back to a clean `0`.

## How It Actually Works

The delay between the actual kill and the metric reflecting it is real and worth internalizing for on-call
work: Kafka doesn't treat a broker as gone the instant a TCP connection drops (a connection can drop
transiently for reasons that have nothing to do with the broker actually being down) — it waits for the
broker to miss enough heartbeats/session checks to be confidently declared unavailable, a deliberate tradeoff
between fast detection and avoiding false-positive failovers over momentary network blips. Once genuinely
detected, the controller (via the KRaft quorum, Level 2 Lesson 1) updates the cluster metadata — new ISR
membership, new leaders where needed — and every broker's local `UnderReplicatedPartitions` count reflects
however many of *its own* led partitions currently have a shrunk ISR, summed cluster-wide by whichever broker
you happen to query (which is why this single broker's JMX endpoint could report a cluster-wide count of 74,
not just its own partitions).

The practical diagnostic sequence this lesson's data maps directly onto a real incident: `JMX/monitoring`
alerts on `UnderReplicatedPartitions > 0` first (the fast, cheap signal to page someone); `kafka-topics.sh
--describe` on the specific affected topics is the next step, confirming exactly which partitions and which
broker dropped out; and the fix, in this lesson's case, is simply restarting the broker — in a real incident,
it might instead require investigating *why* that broker died (OOM, disk failure, network partition) before
it's safe to bring back.

## Common mistakes

- **Expecting `UnderReplicatedPartitions` to react instantly to a broker failure.** As measured directly, there's
  a real detection delay — a monitoring system alerting on this metric should account for that lag rather than
  assuming an immediate signal.
- **Treating a leader failover as itself a problem requiring intervention.** As Level 1, Lesson 7 covered and
  this lesson reconfirms, automatic failover to a healthy in-sync replica is the system working correctly, not
  a symptom to fix — the actual problem is whatever took the original leader down.
- **Reading a single broker's `UnderReplicatedPartitions` value as "that broker's own partitions only."** As
  shown here, the value reported is a count across everything that specific broker currently knows about,
  which in a healthy cluster should agree across every broker you might query, but is worth querying from more
  than one broker during a real investigation to confirm consistency.

## Exercise

On a real multi-broker cluster with a replicated topic, kill one broker (not gracefully — a real `kill -9`,
as this lesson did) and poll `UnderReplicatedPartitions` via JMX every few seconds until it changes, timing
roughly how long real detection takes on your setup. Confirm which partitions' ISRs shrank and whether any
leadership changed, then restart the broker and confirm full recovery.
