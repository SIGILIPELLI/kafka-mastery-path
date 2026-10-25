---
description: "Real consumer fetch benchmarks showing rebalance time dominating a short run, and fetch.min.bytes making throughput worse, not better."
---

# 03 · Consumer Configuration & Fetch Tuning

Producer tuning (Lesson 2) had one honest surprise. Consumer tuning has two, in this lesson's own real
benchmark: rebalance overhead dwarfing the actual data transfer for a short-lived run, and a "wait for more
data before responding" setting making throughput measurably *worse* rather than better.

## The configs that actually matter here

**`fetch.min.bytes`** tells the broker not to respond to a fetch request until at least this many bytes are
available (or `fetch.max.wait.ms` elapses, whichever comes first) — intended to reduce the number of small,
inefficient fetch round-trips when data arrives slowly. **`max.poll.records`** caps how many records a single
`poll()` call returns to the application, controlling how much work happens per poll loop iteration. **Session
and heartbeat timeouts** control how quickly the group coordinator notices a consumer has actually died versus
is just slow — too aggressive, and a momentarily slow consumer gets kicked out and rebalanced away
unnecessarily; too lax, and a genuinely dead consumer's partitions sit unprocessed longer than necessary before
reassignment.

## Worked example: two real consumer-perf-test runs

```text
$ bin/kafka-consumer-perf-test.sh --topic perf-topic --bootstrap-server localhost:9092 --messages 200000
start.time, end.time, data.consumed.in.MB, MB.sec, nMsg, nMsg.sec, rebalance.time.ms, fetch.time.ms, fetch.nMsg.sec
2026-10-24 18:44:59, 2026-10-24 18:45:02, 38.1470, 11.6550, 200000, 61106.0189, 3150, 123, 1626016.2602

$ bin/kafka-consumer-perf-test.sh --topic perf-topic --bootstrap-server localhost:9092 --messages 200000 \
    --command-property fetch.min.bytes=1048576 --command-property fetch.max.wait.ms=500
start.time, end.time, data.consumed.in.MB, MB.sec, nMsg, nMsg.sec, rebalance.time.ms, fetch.time.ms, fetch.nMsg.sec
2026-10-24 18:45:26, 2026-10-24 18:45:30, 38.1470, 10.0705, 200000, 52798.3105, 3159, 629, 317965.0238
```

(Real benchmark numbers from this course's lab broker, 200,000 records already sitting in the topic for both
runs.) Look at the column breakdown in the first run: **`rebalance.time.ms` is 3150** — over three seconds —
while **`fetch.time.ms` is only 123**. The overwhelming majority of this short-lived consumer's total runtime
was spent joining the consumer group and getting a partition assignment, not actually reading data; the real
fetch throughput once that's excluded (`fetch.nMsg.sec`) is a blistering 1.6 million messages/sec. The second
run, with `fetch.min.bytes=1048576` (1 MB) and `fetch.max.wait.ms=500`, shows `fetch.time.ms` jumping to **629**
and fetch throughput *dropping* to about 318K msg/sec — nearly 5x slower at the fetch level than the default.
Forcing the broker to wait for a full megabyte (or 500ms) before responding, on data that was already fully
available the instant the consumer asked, added pure waiting with zero benefit — exactly the opposite of what
this setting is meant to achieve when data arrives in a steady, fast stream rather than a slow trickle.

## How It Actually Works

**Rebalance time dominating a short consumer run** is a direct, visible consequence of the consumer group
protocol (Level 1, Lessons 5 and 6): joining a group, getting a partition assignment, and (for the classic
protocol) potentially waiting out `rebalance.timeout.ms` or other members' join timing, all happen *before* a
single record is fetched. For a short-lived batch job that starts a consumer, reads everything, and exits, this
fixed joining cost can be the majority of total wall-clock time — which is exactly why long-lived, persistent
consumers (that pay the rebalance cost once and then run for hours or days) amortize it away almost entirely,
while this lesson's short perf-test run makes it look disproportionately large.

**`fetch.min.bytes`/`fetch.max.wait.ms`** exist to solve a specific problem: a slow producer trickling in a
few bytes at a time would otherwise cause the consumer to fire off many nearly-empty fetch requests,
round-trip-dominated the same way un-batched producer sends are (Lesson 2). But this lesson's benchmark setup
produces data in one big, fast burst *before* the consumer even starts — there's no slow trickle to batch away,
so forcing a wait (up to 500ms, or until 1MB accumulates) before responding only adds latency the setting was
never meant to spend in this situation. The lesson here generalizes directly from Lesson 2's producer finding:
every batching-style setting trades a *specific* kind of overhead (many small round-trips) for a *specific*
kind of cost (added wait), and that trade is only a net win when the overhead it's fighting is actually present
in your real traffic pattern.

## Common mistakes

- **Copying a "good" `fetch.min.bytes` value from a tuning guide without checking whether your actual traffic
  matches the slow-trickle scenario it was designed for.** As this lesson's real numbers show, it can make
  things measurably worse on different traffic shapes.
- **Benchmarking a short-lived consumer process and treating total throughput as "consumer performance"
  without separating out rebalance time.** `kafka-consumer-perf-test.sh`'s own column breakdown exists
  specifically because the two numbers answer different questions — "how fast did this run finish" vs. "how
  fast can this consumer actually fetch once it's running."
- **Setting session/heartbeat timeouts aggressively low to "detect failures faster" without considering normal
  processing pauses.** A consumer doing legitimately slow per-record work (a heavy database write per message,
  say) can get incorrectly evicted from the group as "dead" if these timeouts don't account for that real
  processing time — `max.poll.interval.ms` specifically exists to separate "is this consumer still alive"
  (heartbeats) from "is this consumer still making progress between polls" (poll interval).

## Exercise

Run `kafka-consumer-perf-test.sh` against a topic with real data already loaded, once with default settings
and once with a `fetch.min.bytes` large enough to matter for your data volume. Compare the `rebalance.time.ms`
and `fetch.time.ms` columns specifically, not just total throughput — and if your result differs from this
lesson's (the larger `fetch.min.bytes` genuinely helping, say), reason about what's different in your traffic
pattern that would explain it.
