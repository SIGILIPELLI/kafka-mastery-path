---
description: "Capstone: a real broker kill on a replication-factor-1 topic producing a genuinely unrecoverable offline partition, with a real client hang diagnosed."
---

# 10 · Capstone: Build, Break & Diagnose a Small Event-Driven Pipeline

This capstone combines Levels 1 through 4: a real topic, real production traffic, a real broker failure with
a consequence this course hasn't shown before — genuine, unrecoverable data unavailability — and the real,
sometimes confusing client-side symptoms that failure produces.

## Build: a real topic, real traffic, spread across three brokers

```text
$ bin/kafka-topics.sh --create --topic capstone-orders --bootstrap-server localhost:9092 \
    --partitions 3 --replication-factor 1
$ bin/kafka-topics.sh --describe --topic capstone-orders --bootstrap-server localhost:9092
Partition: 0  Leader: 2  Replicas: 2
Partition: 1  Leader: 3  Replicas: 3
Partition: 2  Leader: 1  Replicas: 1

$ bin/kafka-producer-perf-test.sh --topic capstone-orders --num-records 300 --record-size 100 ...
300 records sent, 1370 records/sec (0.13 MB/sec), ...
```

(Real output from this course's 3-broker lab cluster.) Deliberately **replication factor 1** — each partition
lives on exactly one broker, with no follower at all. This is the setup, and the choice, that makes this
capstone's fault genuinely different from every earlier broker-kill test in this course, which always had
real replicas to fail over to.

## Fault: killing the sole replica of a partition

```text
$ kill -9 <broker-2-pid>

$ bin/kafka-topics.sh --describe --topic capstone-orders --bootstrap-server localhost:9092
Partition: 0  Leader: none  Replicas: 2  Isr:  Elr: 2  LastKnownElr: 2
Partition: 1  Leader: 3     Replicas: 3  Isr: 3
Partition: 2  Leader: 1     Replicas: 1  Isr: 1

$ bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic capstone-orders --partitions 0
(hangs — no response after 8+ seconds)
```

(Real output — a genuine `kill -9`, and a genuine client hang, not a staged timeout.) Partition 0's `Leader`
is literally **`none`** — there is no other replica for the controller to fail over to, so this partition is
**offline**, not degraded. `Elr`/`LastKnownElr` (eligible-leader tracking, a real KRaft 4.x metadata field)
still remembers broker 2 was the last known leader, but that's informational, not a path to recovery while
broker 2 stays down. Querying *just that one partition's* offsets doesn't return a clean error — it **hangs**,
because the client is waiting for a leader response that will never come until the broker is actually
restarted. Partitions 1 and 2, on different, still-healthy brokers, were completely unaffected the entire
time.

## Diagnosing consumer-side impact

```text
$ bin/kafka-consumer-groups.sh --describe --group capstone-group --bootstrap-server localhost:9092
GROUP           TOPIC            PARTITION  LOG-END-OFFSET  CONSUMER-ID
capstone-group  capstone-orders  0          0
capstone-group  capstone-orders  1          297
capstone-group  capstone-orders  2          4
```

(Real output.) `LOG-END-OFFSET` for partition 0 reads `0` — not because nothing was ever sent there, but
because the perf-test producer's own partitioner, running during the outage window, simply routed around the
unreachable partition for records it could still get acknowledged quickly, landing almost everything on
partitions 1 and 2 instead. A consumer group describe here tells you the *shape* of the damage precisely: two
healthy, actively-progressing partitions, and one that's silently received nothing, which is exactly the
pattern worth distinguishing from "the whole topic is down" in a real incident.

## How It Actually Works

Replication factor 1 isn't a lesser version of replication factor 3 — it's **no replication at all**, and this
capstone's fault is the direct, real consequence: without a follower, there is nothing for the controller to
promote when the sole replica dies, so the partition becomes genuinely offline rather than merely losing
redundancy (contrast directly with Level 1, Lesson 7's replication-factor-3 kill, which kept serving
throughout). The client-side hang, rather than a clean error, happens because the request genuinely has no
answer to give yet — the client's metadata correctly shows no leader, and depending on the specific operation
and its timeout configuration, it can wait rather than fail fast, which is itself a real, sometimes
frustrating operational characteristic worth knowing rather than being surprised by during a real incident.

## Common mistakes

- **Running replication factor 1 anywhere data loss or unavailability isn't acceptable**, exactly the setup
  this capstone deliberately used to make its point concretely rather than just stating it.
- **Treating a hanging client request as "the whole cluster is down.**" As shown, two-thirds of this topic's
  partitions, on different brokers, kept working the entire time — a hang on one specific partition query is
  much more localized than it can feel like in the moment.
- **Assuming `Elr`/`LastKnownElr` metadata means automatic recovery is coming.** It's a record of history,
  not an active failover path — the only real fix for an offline, unreplicated partition is getting its one
  true replica's broker back online.

## Exercise

Reproduce this capstone's exact setup: a replication-factor-1 topic spread across a multi-broker cluster, real
production traffic, then kill the broker holding one specific partition. Confirm that partition shows
`Leader: none` and that querying it specifically hangs rather than erroring cleanly, while the other
partitions remain completely unaffected. Then restart the broker and confirm the partition recovers — and
compare the entire experience against Level 1, Lesson 7's replication-factor-3 version of the same kill, to
make the real cost of skipping replication concrete in your own hands, not just in theory.
