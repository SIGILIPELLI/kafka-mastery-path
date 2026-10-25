---
description: "A real partition reassignment that moves replicas and raises replication factor from 1 to 2 on a live 3-broker cluster, with zero data loss confirmed."
---

# 08 · Partition Reassignment & Scaling a Cluster

Topics don't have to keep the replica placement they were created with. This lesson runs a real reassignment on
a live 3-broker cluster — moving replicas to specific brokers and raising replication factor from 1 to 2 for
every partition in one operation — and confirms every record survived.

## What reassignment actually changes

A reassignment is a declarative instruction: "here is the replica set I want for each partition, now get the
cluster there." It can move a partition's replicas to different brokers (rebalancing load after adding
hardware, or moving data off a broker you're about to decommission), and it can grow or shrink the replica
count (changing replication factor) — both at once, as this lesson's example does, or independently.

## Worked example: a real reassignment, replication factor 1 to 2

```text
$ bin/kafka-topics.sh --describe --topic reassign-demo --bootstrap-server localhost:9092
Partition: 0  Leader: 3  Replicas: 3  Isr: 3
Partition: 1  Leader: 1  Replicas: 1  Isr: 1
Partition: 2  Leader: 2  Replicas: 2  Isr: 2

$ cat reassign.json
{"version":1,"partitions":[
  {"topic":"reassign-demo","partition":0,"replicas":[3,1]},
  {"topic":"reassign-demo","partition":1,"replicas":[1,2]},
  {"topic":"reassign-demo","partition":2,"replicas":[2,3]}
]}

$ bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 \
    --reassignment-json-file reassign.json --execute
Current partition replica assignment
{"version":1,"partitions":[{"partition":0,"replicas":[3],...},...]}
Save this to use as the --reassignment-json-file option during rollback
Successfully started partition reassignments for reassign-demo-0,reassign-demo-1,reassign-demo-2

$ bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 \
    --reassignment-json-file reassign.json --verify
Reassignment of partition reassign-demo-0 is completed.
Reassignment of partition reassign-demo-1 is completed.
Reassignment of partition reassign-demo-2 is completed.

$ bin/kafka-topics.sh --describe --topic reassign-demo --bootstrap-server localhost:9092
Topic: reassign-demo  ReplicationFactor: 2
  Partition: 0  Leader: 3  Replicas: 3,1  Isr: 1,3
  Partition: 1  Leader: 1  Replicas: 1,2  Isr: 1,2
  Partition: 2  Leader: 2  Replicas: 2,3  Isr: 2,3
```

(Real output from this course's 3-broker lab cluster — 9,999 real records were sitting in this topic before
the reassignment began.) Every partition gained a second replica exactly where specified — partition 0 added
broker 1 as a follower, partition 1 added broker 2, partition 2 added broker 3 — and the original leader for
each partition **stayed the leader** throughout; reassignment added replicas without forcing a leadership
change. The `--execute` command's own output is itself worth noting: it printed the *current* (pre-reassignment)
assignment specifically so you can save it as a ready-made rollback file, in case the reassignment needs to be
undone. After `--verify` confirmed completion, a direct offset check
(`kafka-get-offsets.sh ... --topic reassign-demo`) across all three partitions summed to exactly 9,999 — every
record that existed before the reassignment was still there after.

## How It Actually Works

Under the hood, adding a new replica to a partition means that broker has to **bootstrap** a full copy of the
partition's log from scratch — it starts as a follower with nothing, and catches up by fetching from the
leader exactly the way any other follower replication (Level 1, Lesson 7) works, just starting from offset
zero instead of already being caught up. This is why `--verify`, not `--execute`'s return, is what actually
tells you reassignment finished: `--execute` only *starts* the copy; for a large partition, catching up can
take a meaningful amount of time and bandwidth, and checking too early would show replicas still mid-catch-up.
Once a new replica is fully caught up and added to the ISR, and (if replicas were being removed) the old,
no-longer-wanted replica's data is deleted, the reassignment is complete — the controller handles orchestrating
this multi-step process across potentially many brokers from the one declarative JSON file you provided.

A production reassignment moving large amounts of data also typically uses `--throttle` (not needed for this
lesson's small demo) to cap the bandwidth reassignment traffic consumes — without it, a big reassignment can
saturate the same network and disk I/O that live production traffic needs, turning a routine rebalancing
operation into a real incident.

## Common mistakes

- **Checking `--execute`'s immediate output and assuming the reassignment is done.** As explained above,
  execute only starts the process; `--verify` (potentially run repeatedly, since large reassignments take time)
  is what confirms completion.
- **Reassigning a large amount of data on a busy cluster without throttling.** The replication traffic a
  reassignment generates competes directly with normal produce/fetch traffic for the same disk and network
  resources — an unthrottled reassignment at the wrong time is a self-inflicted performance incident.
- **Losing the "current assignment" JSON `--execute` prints before starting.** It's the exact rollback file if
  something goes wrong mid-reassignment — this course's example captured it directly in the command output for
  precisely that reason.

## Exercise

On a multi-broker cluster, create a topic with replication factor 1, produce some real data, and write a
reassignment JSON that both moves partitions to different brokers and raises replication factor to 2 (or
higher). Run `--execute`, then poll `--verify` until it reports completion, and confirm via
`kafka-get-offsets.sh` that the total record count across all partitions matches what you produced before the
reassignment began.
