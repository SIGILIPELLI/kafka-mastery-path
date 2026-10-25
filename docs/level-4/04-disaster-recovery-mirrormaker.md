---
description: "A real MirrorMaker 2 setup replicating a topic between two genuinely independent Kafka clusters, including real live mirroring and renaming."
---

# 04 · Disaster Recovery & Geo-Replication with MirrorMaker 2

Everything so far in this course ran against one cluster. This lesson stands up a **second, completely
independent** Kafka cluster and uses MirrorMaker 2 — the tool bundled with Kafka for exactly this — to
replicate real data between them, the foundation of real disaster-recovery and geo-replication setups.

## Worked example: two real clusters, one real mirrored topic

```text
$ # Cluster A: this course's existing 3-broker cluster (localhost:9092)
$ # Cluster B: a brand-new, independent single-broker cluster (localhost:9099)

$ bin/kafka-topics.sh --create --topic dr-source-topic --bootstrap-server localhost:9092 --partitions 2
$ printf "dr-event-1\ndr-event-2\ndr-event-3\n" | bin/kafka-console-producer.sh --topic dr-source-topic ...

$ cat mm2.properties
clusters = A, B
A.bootstrap.servers = localhost:9092
B.bootstrap.servers = localhost:9099
A->B.enabled = true
A->B.topics = dr-source-topic

$ bin/connect-mirror-maker.sh mm2.properties &

$ bin/kafka-topics.sh --list --bootstrap-server localhost:9099
A.checkpoints.internal
A.dr-source-topic          <- the mirrored topic, automatically renamed
A.heartbeats
heartbeats

$ bin/kafka-console-consumer.sh --topic A.dr-source-topic --from-beginning --bootstrap-server localhost:9099
dr-event-1
dr-event-2
dr-event-3

$ echo "dr-event-live" | bin/kafka-console-producer.sh --topic dr-source-topic --bootstrap-server localhost:9092
$ bin/kafka-get-offsets.sh --bootstrap-server localhost:9099 --topic A.dr-source-topic
A.dr-source-topic:0:4    <- offset advanced: the live event mirrored too
```

(Real output — two genuinely separate Kafka processes, with their own storage, their own metadata, connected
only by MirrorMaker 2.) All three original records crossed to Cluster B exactly as sent, and a **new** record
produced to Cluster A *after* mirroring was already running was picked up and mirrored too — this is
continuous, ongoing replication, not a one-time copy. Notice the topic's name on Cluster B: `A.dr-source-topic`,
not `dr-source-topic` — MM2 prefixes mirrored topics with the **source cluster's alias** by default, a
deliberate naming convention that prevents a topic named identically on both clusters from being ambiguous
about which copy you're looking at.

## How It Actually Works

MirrorMaker 2 is built entirely on **Kafka Connect** (Level 3) — it's not a separate replication protocol,
it's a set of specialized source connectors (`MirrorSourceConnector`, the same class this course's own
Connect workers listed as available by default back in Level 3, Lesson 1) that read from one cluster and
produce to another, using exactly the Connect framework's task model, offset tracking, and fault tolerance.
This is why MM2 configuration looks like Connect configuration (`tasks.max`, replication factors for its own
internal topics) rather than a bespoke tool's own format — it genuinely is Connect, pointed at a second
cluster instead of a non-Kafka external system.

Beyond just copying records, MM2 also maintains **checkpoint** and **offset-sync** topics (visible in the
real topic list above as `A.checkpoints.internal`) specifically so that consumer group offsets can be
translated between clusters — a consumer that was reading cluster A at a certain offset can, after a failover
to cluster B, resume from the *equivalent* position on the mirrored topic, rather than starting over from
scratch or guessing. This offset translation is what separates a real disaster-recovery setup (where a
consumer application can actually fail over and keep working) from simple one-way data copying.

## Common mistakes

- **Treating MirrorMaker 2 as giving you an identical topic name on the target cluster by default.** As shown,
  the source-cluster-alias prefix is the default behavior — consuming applications on the DR side need to know
  to look for `<alias>.<topic>`, not the bare topic name, unless this is explicitly reconfigured.
- **Assuming mirrored data alone is sufficient for consumer failover.** Without also handling offset
  translation (via the checkpoint topics MM2 maintains), a consumer failing over to the mirror cluster has no
  principled way to know where to resume — this is a real, separate piece of DR design beyond "the data is
  copied."
- **Running bidirectional replication (`A->B` and `B->A` both enabled) without understanding the loop
  prevention this requires.** MM2 has mechanisms to avoid infinitely re-mirroring the same data back and forth,
  but configuring active-active replication correctly needs deliberate attention to this, not just flipping
  both directions on.

## Exercise

Stand up two independent single-broker Kafka clusters on your own machine (different ports, different data
directories, as this lesson did), create a topic on one, and configure `connect-mirror-maker.sh` to replicate
it to the other. Confirm existing data mirrors, then produce a new record to the source and confirm it
appears on the target cluster under its renamed topic — real, live, cross-cluster replication, not a one-time
copy.
