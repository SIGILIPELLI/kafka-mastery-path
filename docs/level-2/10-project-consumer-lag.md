---
description: "Project: a real slow consumer diagnosed two ways — classic growing lag, and the more severe case of a consumer too slow to even hold group membership."
---

# 10 · Project: Diagnose a Consumer Lag Problem

Every lesson in this level built one piece of operating a real cluster. This project puts them together on the
single most common real-world Kafka complaint: "the consumer is falling behind." It turns out there are two
genuinely different real symptoms behind that one complaint, and this project hit both of them for real.

## Building the problem

A consumer that does real, slow, per-record work (a database write, a slow downstream call) while the topic
fills up faster than it can keep pace is the textbook lag scenario:

```java
while (true) {
    var records = c.poll(Duration.ofMillis(500));
    for (var r : records) {
        Thread.sleep(50);  // simulate slow per-record processing
    }
}
```

```text
$ bin/kafka-producer-perf-test.sh --topic lag-demo --num-records 5000 --record-size 100 --throughput -1 ...
5000 records sent, 20491.803279 records/sec (1.95 MB/sec), ...
```

5,000 records landed in under a second; the consumer processes one roughly every 50ms.

## Symptom one: classic, measurable lag

```text
$ bin/kafka-consumer-groups.sh --describe --group slow-group --bootstrap-server localhost:9092
Warning: Consumer group 'slow-group' is rebalancing.
GROUP        TOPIC     PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG   CONSUMER-ID
slow-group   lag-demo  0          500             5000            4500  consumer-slow-group-1-...
```

(Real output from this course's lab broker.) This is the textbook case, and it's exactly what
`kafka-consumer-groups.sh --describe` is for: `LOG-END-OFFSET` (5000) minus `CURRENT-OFFSET` (500) is `LAG`
(4500) — the consumer has only processed 500 of 5000 available records. At roughly 20 records/sec (the real
rate `Thread.sleep(50)` per record implies), clearing 4,500 records of lag would take well over three minutes
— a real, quantifiable backlog, not a vague feeling that "things seem slow."

## Symptom two: too slow to even hold membership

Re-running a similar setup produced a genuinely different, more severe symptom:

```text
$ bin/kafka-consumer-groups.sh --describe --group slow-group --bootstrap-server localhost:9092
Warning: Consumer group 'slow-group' is rebalancing.
GROUP        TOPIC     PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG   CONSUMER-ID
slow-group   lag-demo  0          -               3000            -    consumer-slow-group-1-f6008391-...

$ # moments later, same command
GROUP        TOPIC     PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG   CONSUMER-ID
slow-group   lag-demo  0          -               3000            -    consumer-slow-group-1-734833d2-...
```

(Also real — note the **consumer ID changed** between the two checks, moments apart.) `CURRENT-OFFSET` and
`LAG` are showing **dashes**, not numbers — this consumer hasn't successfully committed a single offset.
Worse, its member ID is different on each check: it's being kicked out of the group and **rejoining as a
brand-new member** repeatedly, never stabilizing long enough to process anything or commit progress. This is a
genuinely different failure mode from symptom one — not "slow but steadily working through a backlog," but
"too slow (or too disrupted) to even remain a group member," which looks superficially similar in a quick
glance (the consumer "isn't keeping up") but has a different cause and a different fix.

## How It Actually Works

Symptom one is pure throughput mismatch: the consumer *is* a stable, functioning group member, it's just
processing slower than records arrive — the fix is either speeding up per-record processing, adding more
consumers (if there are more partitions to parallelize across), or accepting the backlog will take time to
clear. Symptom two is a **group membership** problem: something — genuinely slow per-poll processing exceeding
`max.poll.interval.ms`, or in this project's case, real execution delays in the lab environment itself —
prevented the consumer from calling `poll()` again quickly enough to prove to the coordinator it was still
alive, triggering eviction. Once evicted, the consumer has to rejoin from scratch, and if whatever caused the
original delay is still happening, it can be evicted again before ever catching up enough to commit a single
offset — a real rejoin loop, visible directly in the changing consumer ID between checks.

Distinguishing these two from `kafka-consumer-groups.sh --describe` alone is straightforward once you know
what to look for: real numbers in `CURRENT-OFFSET`/`LAG` that are large but *stable or shrinking* over repeated
checks is symptom one; dashes, a "rebalancing" warning, and a **changing consumer ID** across repeated checks
is symptom two — and they call for different fixes (optimize or scale out processing, vs. investigate why
`poll()` isn't being called often enough and consider `max.poll.records`/`max.poll.interval.ms` tuning).

## Common mistakes

- **Treating every "the consumer is behind" report as symptom one.** As this project shows, a consumer that
  can't even hold group membership looks superficially similar in a casual glance but needs a completely
  different fix — jumping straight to "add more consumers" or "optimize processing" without checking for a
  changing consumer ID first can miss the real cause entirely.
- **Reacting to a single `--describe` snapshot instead of comparing repeated checks.** Symptom two is only
  visible *as a pattern* — one snapshot showing dashes could be an ordinary mid-rebalance moment; only a
  second check moments later, showing a different consumer ID, confirms a real rejoin loop.
- **Assuming `CURRENT-OFFSET`/`LAG` showing dashes means "no data has moved."** It specifically means this
  *group* hasn't committed an offset yet — the topic's `LOG-END-OFFSET` can still be climbing normally while
  the consumer side shows nothing, which is exactly the gap this project's two symptoms both exploit in
  different ways.

## Exercise

Reproduce symptom one: a consumer doing deliberate, real per-record delay against a topic with more records
than it can process quickly, and watch `LAG` with repeated `--describe` calls — confirm it's a stable,
decreasing number as the consumer slowly works through the backlog. Then, separately, try to reproduce symptom
two by making per-record delay large enough (or processing a large-enough batch per `poll()`) that it
approaches your `max.poll.interval.ms` — watch for a changing consumer ID across checks, which is the
real, concrete signal distinguishing "slow but working" from "too slow to stay in the group at all."
