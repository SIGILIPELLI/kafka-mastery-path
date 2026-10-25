---
description: "What a Kafka broker does, and how the KRaft controller quorum (which replaced ZooKeeper entirely as of Kafka 4.0) actually works."
---

# 02 · Brokers, Clusters & KRaft

For most of Kafka's history, every cluster needed a separate system — Apache ZooKeeper — just to track cluster
metadata: which brokers exist, which partition leader is where, what the current topic configuration is. As of
**Kafka 4.0, ZooKeeper support was removed entirely.** Every broker now participates in (or talks to) a
**KRaft** metadata quorum built directly into Kafka itself. This lesson looks at both what a broker does and
what that quorum actually looks like, running for real.

## What a broker actually does

A **broker** is a single Kafka server process: it holds some set of partition logs on local disk, serves
produce and fetch requests for the partitions it leads, and replicates data for partitions where it's a
follower. A **cluster** is a set of brokers working together, each one typically leading some partitions and
following others — there's no single, special "the server"; every write and read routes to whichever broker
currently leads the relevant partition.

## Worked example: a real broker's role configuration and a real quorum status

```text
$ cat config/server.properties | grep -E "process.roles|node.id|controller"
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=localhost:9093
controller.listener.names=CONTROLLER

$ bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status
ClusterId:              DfcdfOpkTYe9c0fXQmCgxQ
LeaderId:               1
LeaderEpoch:            1
HighWatermark:          1065
MaxFollowerLag:         0
MaxFollowerLagTimeMs:   0
CurrentVoters:          [{"id": 1, "directoryId": "ddnwqk4sQTmMSOUxo0xWhg", "endpoints": ["CONTROLLER://localhost:9093"]}]
CurrentObservers:       []
```

(Real output from this course's lab broker.) `process.roles=broker,controller` is the key line: this single
node is running **both** roles at once — a `--standalone` configuration suitable for development, where one
process both serves client traffic (`broker`) and participates in the metadata quorum (`controller`). A real
production cluster typically dedicates separate nodes to each role, or at least runs multiple controller nodes
for fault tolerance, rather than combining both on one process as this lab does for simplicity.
`kafka-metadata-quorum.sh describe --status` queries the **controller quorum itself**, not the broker's data —
`LeaderId: 1` names which controller node currently leads metadata decisions, `CurrentVoters` lists every node
with a vote in that quorum (just one, here, since this is a single-node lab), and `HighWatermark` tracks how far
the quorum's internal metadata log has been durably committed — genuinely the same leader/follower/log model
from Lesson 1, just applied to Kafka's *own* metadata instead of application data.

## How It Actually Works

KRaft implements the **Raft consensus algorithm** directly inside Kafka to replicate cluster metadata (topic
configs, partition assignments, ACLs, broker membership) across a small set of **controller** nodes, the same
way application data is replicated across partition replicas. This is architecturally cleaner than the old
ZooKeeper-based design for a simple reason: before KRaft, Kafka had **two** separate replication/consensus
systems running side by side — ZooKeeper's own protocol for metadata, and Kafka's own leader/ISR model for
partition data — which meant two systems to operate, upgrade, and reason about for failure scenarios. KRaft
collapses this to one: metadata is just another Raft-replicated log, using the same mental model (leaders,
followers, a high-water mark) this course already introduces for ordinary topic partitions in Lesson 1.

The practical consequence worth understanding is **controller quorum size**. A KRaft quorum needs a **majority**
of its voters available to elect a leader and keep making progress — exactly like Raft in general, and exactly
like why ZooKeeper deployments traditionally used an odd number of nodes (3 or 5). A single-voter quorum, as in
this lab, has zero fault tolerance: if that one controller node goes down, the entire cluster's metadata plane
is unavailable, even though this is perfectly fine for local development. A production cluster uses an odd
number of controller nodes (commonly 3) specifically so the quorum can lose one node and still have a majority
to keep electing a leader and committing metadata changes.

## Common mistakes

- **Running a single-controller-voter KRaft setup in production "because it worked in development."** As shown
  above, a one-voter quorum has no tolerance for that controller going down — always use an odd-numbered,
  multi-node controller quorum (typically 3) for anything beyond local testing.
- **Confusing the controller quorum's health with broker (data-plane) health.** `kafka-metadata-quorum.sh
  describe` reports on the *metadata* quorum specifically; a broker can be serving produce/fetch traffic
  perfectly fine even while the controller quorum has its own, separate state to check — they're related but
  distinct failure domains.
- **Assuming "no ZooKeeper" means "no consensus system at all."** KRaft didn't remove the need for distributed
  consensus over cluster metadata — it moved that consensus inside Kafka itself, using Raft instead of
  ZooKeeper's own protocol (ZAB). The operational need for an odd, fault-tolerant quorum size didn't go away.

## Exercise

Run `kafka-metadata-quorum.sh --bootstrap-server <host:port> describe --status` against any Kafka 4.x cluster you
have access to (or this lesson's own lab setup) and identify: the current leader's node ID, how many voters are
in `CurrentVoters`, and whether that number is odd (as a healthy quorum size should be). If you only have a
single-node lab like this lesson's, explain in your own words what would happen to the whole cluster if that one
node's controller process crashed.
