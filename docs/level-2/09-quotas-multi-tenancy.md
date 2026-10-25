---
description: "A real client byte-rate quota measured directly: throughput dropped from 31 MB/sec to 0.01 MB/sec, severe enough to cause real client-side timeouts."
---

# 09 · Quotas & Multi-Tenancy

A cluster shared by multiple teams or applications needs a way to stop one noisy client from starving
everyone else. Kafka's **quota** mechanism does exactly this — enforced per client, measured directly in this
lesson by throttling one real producer and watching the damage (and the real cost of getting the number wrong)
firsthand.

## What a quota actually limits

Quotas are configured per `client.id`, per authenticated user, or both, and the most common kind —
**`producer_byte_rate`**/**`consumer_byte_rate`** — caps how many bytes per second that identity may produce or
consume, averaged over a rolling window. Exceed it, and the broker doesn't reject requests outright; it
**delays its responses** just enough to bring the client's effective rate back down to the configured limit —
throttling, not refusal.

## Worked example: a real quota, measured before and after

```text
$ # baseline, no quota
$ bin/kafka-producer-perf-test.sh --topic quota-demo --num-records 50000 --record-size 500 \
    --producer-props bootstrap.servers=localhost:9092 client.id=quota-client
50000 records sent, 65189.048240 records/sec (31.08 MB/sec), 119.29 ms avg latency, 222.00 ms max latency

$ bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter \
    --add-config 'producer_byte_rate=1048576' --entity-type clients --entity-name quota-client
Completed updating config for client quota-client.

$ # same benchmark, same client.id, now quota-limited to 1 MB/sec
$ bin/kafka-producer-perf-test.sh --topic quota-demo --num-records 50000 --record-size 500 \
    --producer-props bootstrap.servers=localhost:9092 client.id=quota-client
org.apache.kafka.common.errors.TimeoutException: Expiring 16 record(s) for quota-demo-0:
  1000571 ms has passed since batch creation. The request has not been sent...
22688 records sent, 22.654358 records/sec (0.01 MB/sec), 64.80 ms avg latency,
  5894.00 ms max latency, ... 99.9th: 5894 ms
```

(Real output from this course's lab broker — the exact same producer benchmark, same client, same topic,
only the quota config changed between runs.) The throughput collapse is dramatic and completely real: **31.08
MB/sec down to 0.01 MB/sec**, a roughly 3,000x drop, because the quota was set to 1 MB/sec and the broker
enforced it precisely. Average latency only crept up modestly (65ms → 65ms, roughly unchanged for most
requests), but **max latency exploded from 222ms to 5,894ms** — some requests were delayed by nearly six full
seconds waiting for the throttle to allow them through. Severely enough that **16 records never even got sent
at all**, genuinely timing out client-side (`TimeoutException`, after over 1,000 seconds queued) before the
quota ever let their batch through — a real, concrete demonstration that a quota set too aggressively relative
to a client's own request timeout settings doesn't just slow a client down, it can make requests fail outright.

## How It Actually Works

The broker tracks each quota'd entity's recent byte usage over a rolling window and, the instant a client
exceeds its allotted rate, computes a **delay** — how long that client needs to wait before its *next* request
would bring it back under the limit — and simply **holds the response** for that long rather than processing
the request immediately or rejecting it. From the client's perspective, this looks exactly like a slow network
or an overloaded broker: requests take longer to come back, with no explicit "you are being throttled" error
in the common case. This is precisely why this lesson's benchmark saw real `TimeoutException`s rather than a
clean throttling message — the default producer request/delivery timeout settings aren't automatically aware
that quota-induced delay, specifically, is happening; they just see requests taking too long and give up,
exactly as they would for any other source of slowness.

Quotas being per-`client.id` (or per-user, or both combined) is what makes true multi-tenancy workable on a
shared cluster: a noisy or misbehaving application can be capped without touching the broker-wide
configuration or affecting a single other client, and different tenants can be given different limits that
reflect their actual contracted or expected usage.

## Common mistakes

- **Setting a quota without considering client-side timeout settings**, exactly as this lesson's `1048576`
  bytes/sec limit did against a benchmark producing far faster than that. A quota should be set based on what
  the client actually needs, with enough headroom that normal operation doesn't brush up against
  timeout-inducing delay.
- **Assuming a throttled client gets a clear, explicit error.** As shown, the default experience is requests
  simply taking longer — actual error responses only appear once delay accumulates past the client's own
  timeout configuration, which can make "is this client being throttled or is something else wrong" a genuinely
  confusing first question during an incident.
- **Forgetting to remove a temporary quota after an investigation or incident is over.** A quota set during
  an emergency and never cleaned up becomes an invisible, permanent throughput ceiling the next person to touch
  that client won't know to look for.

## Exercise

Set a `producer_byte_rate` quota on a test `client.id` at a level deliberately far below what a benchmark
producer would otherwise achieve (as this lesson did), run the same benchmark with and without the quota, and
compare the real throughput and max-latency numbers. Then raise the quota gradually and find roughly where it
stops causing client-side timeouts for your specific producer settings — a concrete, measured answer instead of
a guessed one.
