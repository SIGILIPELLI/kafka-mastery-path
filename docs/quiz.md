---
title: "Final Quiz"
description: "Test what you have learned across all four levels of Kafka Mastery Path with a 19-question final quiz."
---

# Final Quiz

19 questions drawn from all four levels of this course. You need **70% (14 out of 19)** to pass —
you can retake it as many times as you like. Your best result is saved in this browser.

<div id="mp-quiz-root"></div>

<script type="application/json" id="mp-quiz-data">
[
  {
    "question": "Why does Kafka guarantee ordering only within a partition?",
    "options": [
      "Each partition is an independent append-only log; there is no global order across partitions",
      "Consumers reorder",
      "Brokers sort",
      "Keys are random"
    ],
    "correctIndex": 0,
    "explanation": "Use a key to keep related events together."
  },
  {
    "question": "Which consumers are idle when 5 consumers share a 3-partition topic?\n```python\nprint(5-3)\n```\n",
    "options": [
      "2",
      "5",
      "3",
      "0"
    ],
    "correctIndex": 0,
    "explanation": "Parallelism is bounded by partitions. Verified by running it."
  },
  {
    "question": "Partition ends are [30, 10, 5] and committed offsets are [25, 10, 0]. Total lag?\n```python\nprint(sum(e-c for e,c in zip([30,10,5],[25,10,0])))\n```\n",
    "options": [
      "5",
      "45",
      "10",
      "15"
    ],
    "correctIndex": 2,
    "explanation": "Lag = end offset minus committed offset. Verified by running it."
  },
  {
    "question": "A producer retries after a lost ack and duplicates appear. What fixes it?",
    "options": [
      "Compression",
      "acks=0",
      "More partitions",
      "enable.idempotence=true (sequence numbers dedupe within a partition)"
    ],
    "correctIndex": 3,
    "explanation": "Safe retries."
  },
  {
    "question": "How do you avoid data loss when a broker dies?",
    "options": [
      "More consumers",
      "RF 1",
      "acks=1 only",
      "Replication factor 3, acks=all and min.insync.replicas=2"
    ],
    "correctIndex": 3,
    "explanation": "Durability settings."
  },
  {
    "question": "Consumers commit offsets before processing and lose messages on a crash. What's the fix?",
    "options": [
      "Commit earlier",
      "Disable commits",
      "Commit after processing (at-least-once) and make handlers idempotent",
      "Add partitions"
    ],
    "correctIndex": 2,
    "explanation": "Order of commit matters."
  },
  {
    "question": "A consumer is repeatedly removed from its group. What do you check?",
    "options": [
      "ACLs",
      "Compaction",
      "Poll interval/processing time (max.poll.interval.ms), session timeout and heartbeats",
      "Retention"
    ],
    "correctIndex": 2,
    "explanation": "Rebalance storms."
  },
  {
    "question": "Which topic setting keeps only the latest value per key?",
    "options": [
      "retention.ms",
      "acks",
      "cleanup.policy=compact",
      "delete"
    ],
    "correctIndex": 2,
    "explanation": "Changelogs and tables."
  },
  {
    "question": "How do you delete a key from a compacted topic?",
    "options": [
      "Produce a tombstone (null value)",
      "Drop topic",
      "Delete offset",
      "Set TTL"
    ],
    "correctIndex": 0,
    "explanation": "Compaction removes it later."
  },
  {
    "question": "An exactly-once pipeline writes to an external database. Does Kafka transactions cover it?",
    "options": [
      "No: use an idempotent or transactional sink; Kafka's exactly-once covers Kafka-to-Kafka",
      "Yes",
      "Only keys",
      "Only reads"
    ],
    "correctIndex": 0,
    "explanation": "Idempotent consumers."
  },
  {
    "question": "What must consumers set to skip aborted transactional records?",
    "options": [
      "auto commit",
      "earliest",
      "isolation.level=read_committed",
      "read_uncommitted"
    ],
    "correctIndex": 2,
    "explanation": "Transaction visibility."
  },
  {
    "question": "Two streams must be joined by key but fail with an error. What's usually required?",
    "options": [
      "More RAM",
      "Co-partitioning: same keys and the same partition count (repartition if needed)",
      "A bigger broker",
      "Compaction"
    ],
    "correctIndex": 1,
    "explanation": "Join prerequisite."
  },
  {
    "question": "A Kafka Streams instance restarts and takes ages to serve. What reduces this?",
    "options": [
      "acks=all",
      "More topics",
      "Standby replicas and smaller state; state is rebuilt from the changelog",
      "Compression"
    ],
    "correctIndex": 2,
    "explanation": "Fast failover."
  },
  {
    "question": "A schema change breaks consumers. What prevents it?",
    "options": [
      "Free-form JSON",
      "Quotas",
      "A schema registry with compatibility checks (e.g. adding fields with defaults)",
      "More partitions"
    ],
    "correctIndex": 2,
    "explanation": "Contracts."
  },
  {
    "question": "A poison message blocks a consumer. What's the pattern?",
    "options": [
      "Retry a bounded number of times, then send to a dead-letter topic with error context",
      "Infinite retry",
      "Skip silently",
      "Delete topic"
    ],
    "correctIndex": 0,
    "explanation": "Keep the stream moving."
  },
  {
    "question": "You need cross-region disaster recovery. Which tool?",
    "options": [
      "MirrorMaker 2 (replicating topics and translating offsets)",
      "Compaction",
      "Connect only",
      "Streams"
    ],
    "correctIndex": 0,
    "explanation": "Plan RPO/RTO."
  },
  {
    "question": "Roughly how many TB of disk for 20 MB/s, RF 3, 3-day retention?\n```python\nprint(round(20e6*86400*3*3/1e12,1))\n```\n",
    "options": [
      "1.6",
      "52",
      "15.6",
      "5.2"
    ],
    "correctIndex": 2,
    "explanation": "throughput x time x replication. Verified by running it."
  },
  {
    "question": "Producers overload the cluster for other teams. What's the control?",
    "options": [
      "Per-client/user quotas (produce/fetch/request)",
      "TLS",
      "Fewer topics",
      "Compaction"
    ],
    "correctIndex": 0,
    "explanation": "Multi-tenancy."
  },
  {
    "question": "What's the safe way to upgrade brokers?",
    "options": [
      "Skip staging",
      "Rolling restarts one at a time, upgrade binaries before bumping metadata/protocol versions",
      "All at once",
      "Delete logs"
    ],
    "correctIndex": 1,
    "explanation": "Zero downtime."
  }
]
</script>

## Coding challenges

Try these on your own computer, then compare your output. Nothing you write is sent anywhere.

### Partitions, consumer groups and lag

Model a topic with key-based partitioning and a consumer group: assignment() gives each partition to exactly one member (round-robin), poll() reads from the committed offset, commit() advances offsets and lag() reports end minus committed per partition. Check the rebalance with more members than partitions.

**Starter code**

```java
class Partition:
    def __init__(self):
        self.log = []                              # list of (key, value); offset = index

class Topic:
    def __init__(self, partitions):
        self.parts = [Partition() for _ in range(partitions)]

    def produce(self, key, value):
        p = sum(key.encode()) % len(self.parts)    # stable key -> partition (toy hash)
        self.parts[p].log.append((key, value))
        return p, len(self.parts[p].log) - 1

class Group:
    """A consumer group: each partition is owned by exactly one member; offsets are committed per partition."""
    def __init__(self, topic):
        self.topic = topic
        self.members = []
        self.committed = {i: 0 for i in range(len(topic.parts))}

    def assignment(self):
        # {member: [partition ids]} - each partition owned by exactly one member (round-robin by partition id)
        return {}

    def poll(self, member, max_records=10):
        # records for the member's partitions starting at the committed offset: [(partition, offset, (key, value))]
        return []

    def commit(self, records):
        # advance each partition's committed offset past the highest offset seen
        pass

    def lag(self):
        # {partition: end offset - committed offset}
        return {}

t = Topic(3)
for k in ["ada", "bo", "cy", "ada", "bo", "ada", "dee"]:
    t.produce(k, "event-" + k)
g = Group(t)
g.members = ["c1", "c2"]
print("assignment:", g.assignment())
print("lag:", g.lag())
r1 = g.poll("c1")
print("c1 got:", [(i, o) for i, o, _ in r1])
g.commit(r1)
print("lag after c1 commit:", g.lag())
g.members = ["c1", "c2", "c3", "c4"]                    # rebalance: more members than partitions
print("assignment:", g.assignment())
print("same key -> same partition:", len({t.produce("ada", "x")[0] for _ in range(5)}) == 1)
```

**Expected output**

```text
assignment: {'c1': [0, 2], 'c2': [1]}
lag: {0: 3, 1: 1, 2: 3}
c1 got: [(0, 0), (0, 1), (0, 2), (2, 0), (2, 1), (2, 2)]
lag after c1 commit: {0: 0, 1: 1, 2: 0}
assignment: {'c1': [0], 'c2': [1], 'c3': [2], 'c4': []}
same key -> same partition: True
```

<details><summary>Show solution and explanation</summary>

<pre><code>class Partition:
    def __init__(self):
        self.log = []                              # list of (key, value); offset = index

class Topic:
    def __init__(self, partitions):
        self.parts = [Partition() for _ in range(partitions)]

    def produce(self, key, value):
        p = sum(key.encode()) % len(self.parts)    # stable key -&gt; partition (toy hash)
        self.parts[p].log.append((key, value))
        return p, len(self.parts[p].log) - 1

class Group:
    &quot;&quot;&quot;A consumer group: each partition is owned by exactly one member; offsets are committed per partition.&quot;&quot;&quot;
    def __init__(self, topic):
        self.topic = topic
        self.members = []
        self.committed = {i: 0 for i in range(len(topic.parts))}

    def assignment(self):
        out = {m: [] for m in self.members}
        for i in range(len(self.topic.parts)):
            out[self.members[i % len(self.members)]].append(i)       # round-robin
        return out

    def poll(self, member, max_records=10):
        got = []
        for i in self.assignment()[member]:
            log = self.topic.parts[i].log
            for off in range(self.committed[i], min(len(log), self.committed[i] + max_records)):
                got.append((i, off, log[off]))
        return got

    def commit(self, records):
        for i, off, _ in records:
            self.committed[i] = max(self.committed[i], off + 1)

    def lag(self):
        return {i: len(self.topic.parts[i].log) - self.committed[i] for i in self.committed}

t = Topic(3)
for k in [&quot;ada&quot;, &quot;bo&quot;, &quot;cy&quot;, &quot;ada&quot;, &quot;bo&quot;, &quot;ada&quot;, &quot;dee&quot;]:
    t.produce(k, &quot;event-&quot; + k)
g = Group(t)
g.members = [&quot;c1&quot;, &quot;c2&quot;]
print(&quot;assignment:&quot;, g.assignment())
print(&quot;lag:&quot;, g.lag())
r1 = g.poll(&quot;c1&quot;)
print(&quot;c1 got:&quot;, [(i, o) for i, o, _ in r1])
g.commit(r1)
print(&quot;lag after c1 commit:&quot;, g.lag())
g.members = [&quot;c1&quot;, &quot;c2&quot;, &quot;c3&quot;, &quot;c4&quot;]                    # rebalance: more members than partitions
print(&quot;assignment:&quot;, g.assignment())
print(&quot;same key -&gt; same partition:&quot;, len({t.produce(&quot;ada&quot;, &quot;x&quot;)[0] for _ in range(5)}) == 1)
</code></pre>

<p>Each partition is owned by one member of the group, offsets are committed per partition and lag is end offset minus committed offset; extra members get no partitions. This is a teaching model, not the Kafka client. Verified by running the solution.</p>
</details>

### Idempotent producer and at-least-once consumer

Complete Broker.append (dedupe by (producer, sequence) when enabled) and Consumer.run (process then commit; a crash before commit means the record is processed again). Compare plain retries with idempotence, and a naive consumer with an idempotent handler.

**Starter code**

```java
class Producer:
    """Idempotent producer model: retries after a lost ack must not create duplicates."""
    def __init__(self, broker):
        self.broker = broker
        self.seq = 0

    def send(self, value, fail_ack_times=0):
        seq = self.seq
        self.seq += 1
        attempts = 0
        while True:
            attempts += 1
            ack = self.broker.append(("p1", seq), value)
            if attempts > fail_ack_times:             # the first N acks are "lost" on the network
                return attempts

class Broker:
    def __init__(self, dedupe):
        self.log, self.seen, self.dedupe = [], set(), dedupe

    def append(self, pid_seq, value):
        # store value; when self.dedupe is on, a (producer, sequence) pair already seen must not be
        # stored again but should still be acknowledged. Always return True.
        return True

class Consumer:
    """At-least-once consumer: commits after processing; a crash before the commit means reprocessing."""
    def __init__(self, log, handler):
        self.log, self.handler, self.committed = log, handler, 0

    def run(self, crash_after=None):
        # process records from the committed offset, committing AFTER each is handled; when crash_after
        # records have been handled in this call, return "crashed" before committing that last one;
        # otherwise return "done" at the end of the log
        return "done"

for dedupe in (False, True):
    b = Broker(dedupe); p = Producer(b)
    p.send("a"); p.send("b", fail_ack_times=2); p.send("c")
    print("idempotence" if dedupe else "plain retries", "->", b.log)

b = Broker(True); p = Producer(b)
for v in "abc": p.send(v)
seen = []
c = Consumer(b.log, seen.append)
print(c.run(crash_after=2), seen, "committed", c.committed)
print(c.run(), seen, "committed", c.committed)
effects = {}
def idempotent(v): effects[v] = effects.get(v, 0) + 1
c2 = Consumer(b.log, lambda v: idempotent(v)); c2.run(crash_after=2); c2.run()
print("side effects by key (naive):", effects)
done = set(); applied = []
def dedup_handler(v):
    if v not in done:
        done.add(v); applied.append(v)
c3 = Consumer(b.log, dedup_handler); c3.run(crash_after=2); c3.run()
print("applied with idempotent handler:", applied)
```

**Expected output**

```text
plain retries -> ['a', 'b', 'b', 'b', 'c']
idempotence -> ['a', 'b', 'c']
crashed ['a', 'b'] committed 1
done ['a', 'b', 'b', 'c'] committed 3
side effects by key (naive): {'a': 1, 'b': 2, 'c': 1}
applied with idempotent handler: ['a', 'b', 'c']
```

<details><summary>Show solution and explanation</summary>

<pre><code>class Producer:
    &quot;&quot;&quot;Idempotent producer model: retries after a lost ack must not create duplicates.&quot;&quot;&quot;
    def __init__(self, broker):
        self.broker = broker
        self.seq = 0

    def send(self, value, fail_ack_times=0):
        seq = self.seq
        self.seq += 1
        attempts = 0
        while True:
            attempts += 1
            ack = self.broker.append((&quot;p1&quot;, seq), value)
            if attempts &gt; fail_ack_times:             # the first N acks are &quot;lost&quot; on the network
                return attempts

class Broker:
    def __init__(self, dedupe):
        self.log, self.seen, self.dedupe = [], set(), dedupe

    def append(self, pid_seq, value):
        if self.dedupe and pid_seq in self.seen:
            return True                                # duplicate detected, already stored
        self.seen.add(pid_seq)
        self.log.append(value)
        return True

class Consumer:
    &quot;&quot;&quot;At-least-once consumer: commits after processing; a crash before the commit means reprocessing.&quot;&quot;&quot;
    def __init__(self, log, handler):
        self.log, self.handler, self.committed = log, handler, 0

    def run(self, crash_after=None):
        processed = 0
        while self.committed &lt; len(self.log):
            self.handler(self.log[self.committed])
            processed += 1
            if crash_after is not None and processed == crash_after:
                return &quot;crashed&quot;                       # processed but not yet committed
            self.committed += 1
        return &quot;done&quot;

for dedupe in (False, True):
    b = Broker(dedupe); p = Producer(b)
    p.send(&quot;a&quot;); p.send(&quot;b&quot;, fail_ack_times=2); p.send(&quot;c&quot;)
    print(&quot;idempotence&quot; if dedupe else &quot;plain retries&quot;, &quot;-&gt;&quot;, b.log)

b = Broker(True); p = Producer(b)
for v in &quot;abc&quot;: p.send(v)
seen = []
c = Consumer(b.log, seen.append)
print(c.run(crash_after=2), seen, &quot;committed&quot;, c.committed)
print(c.run(), seen, &quot;committed&quot;, c.committed)
effects = {}
def idempotent(v): effects[v] = effects.get(v, 0) + 1
c2 = Consumer(b.log, lambda v: idempotent(v)); c2.run(crash_after=2); c2.run()
print(&quot;side effects by key (naive):&quot;, effects)
done = set(); applied = []
def dedup_handler(v):
    if v not in done:
        done.add(v); applied.append(v)
c3 = Consumer(b.log, dedup_handler); c3.run(crash_after=2); c3.run()
print(&quot;applied with idempotent handler:&quot;, applied)
</code></pre>

<p>Sequence numbers let the broker drop duplicate retries, while commit-after-processing gives at-least-once delivery, so side effects must be idempotent. Verified by running the solution.</p>
</details>
