---
description: "Real Kafka JMX metrics queried off a live broker: under-replicated partitions, request handler idle time, and message throughput, before and after load."
---

# 05 · Monitoring: The Metrics That Actually Matter

Kafka exposes dozens of JMX metrics, and most monitoring guides list them without showing what a real value
actually looks like. This lesson queries several directly off a real running broker — once quiet, once under
real load — so the numbers mean something concrete rather than abstract.

## The handful that matter most

**`UnderReplicatedPartitions`** (broker-level): the count of partitions where the ISR has shrunk below the
full replica set (Level 1, Lesson 7) — healthy is zero, and any sustained nonzero value is a real, active
replication problem worth paging someone about. **`RequestHandlerAvgIdlePercent`**: how much of the time the
broker's request-handling thread pool sits idle — high is good (plenty of spare capacity), a value dropping
toward zero under normal load means the broker is running out of processing threads for incoming requests.
**`MessagesInPerSec`**/**`BytesInPerSec`**: real throughput, the first number worth checking against capacity
planning assumptions (Level 4, Lesson 2). **Consumer lag** (not a broker JMX metric at all — it's computed by
comparing a partition's log-end-offset to a consumer group's committed offset, exactly what
`kafka-consumer-groups.sh --describe` already showed in Level 1, Lesson 5) rounds out the list of what to watch
first.

## Worked example: real JMX values, quiet and under load

```text
$ bin/kafka-jmx.sh --jmx-url service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi \
    --object-name 'kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions' --one-time true
"time","...UnderReplicatedPartitions:Value"
1792847993386,0

$ bin/kafka-jmx.sh ... --object-name 'kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent' --one-time true
"time",...,"...MeanRate",...
1792848001601,...,0.9913635856270023,...

$ # before any traffic
$ bin/kafka-jmx.sh ... --object-name 'kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec' --one-time true
1792848012155,0,messages,0.0,0.0,0.0,0.0,SECONDS

$ # after producing 50,000 real records to this broker
$ bin/kafka-jmx.sh ... --object-name 'kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec' --one-time true
1792848030302,50000,messages,55.09...,162.55...,989.29...,735.63...,SECONDS
```

(Real output from this course's lab broker, queried over real JMX — `JMX_PORT=9999` was set when starting the
broker, and `com.sun.management.jmxremote.authenticate=false` is a default this lab relies on for simplicity,
never appropriate for anything beyond local development.) `UnderReplicatedPartitions` reading `0` on a healthy,
quiet cluster is exactly the boring, correct baseline — Level 2 Lesson 1's quorum-loss experiment, if repeated
with replicated topics present, would be the kind of event that pushes this number above zero. The idle-percent
metric's `0.99...` mean rate confirms the request handler threads are spending essentially all their time idle
on this quiet lab broker — consistent with handling this course's occasional test commands, not a real
production load. `MessagesInPerSec` tells the clearest before/after story: a flat `0` before any traffic, a
real `Count` of exactly `50000` and a `MeanRate` near `989` messages/sec immediately after running a real
50,000-record producer benchmark against it — the metric is reporting real, observed throughput, not a
simulated or placeholder number.

## How It Actually Works

Every one of these metrics is backed by Kafka's internal **Yammer/Dropwizard-style metrics registry**, exposed
automatically as JMX MBeans the moment the broker process starts — `JmxReporter` (visible in this course's
earlier broker startup/shutdown logs) is the component that registers and unregisters them. This is exactly
why `MessagesInPerSec` read `0` immediately after this broker's restart: JMX metrics live in the **JVM
process's memory**, not on disk, so a restart genuinely resets every counter and rate to its initial state —
historical throughput before a restart isn't recoverable from JMX alone, which is precisely why real production
setups ship these metrics continuously to an external time-series system (Prometheus via a JMX exporter,
Datadog, etc.) rather than relying on point-in-time JMX queries the way this lesson's lab demonstration does.

The **rate** metrics (`OneMinuteRate`, `FiveMinuteRate`, `MeanRate`) are exponentially-weighted moving averages
over their respective windows, the exact same statistical shape as the OS load-average concept from the Linux
Mastery Path's performance lessons — which is why, immediately after a burst of 50,000 records, the
`OneMinuteRate` (735/sec) and `MeanRate` (989/sec, averaged since process start) differ from each other: they're
genuinely different time windows over the same underlying event stream, not two measurements of the same thing
computed differently.

## Common mistakes

- **Treating a single JMX snapshot as a trend.** `UnderReplicatedPartitions` reading 0 right now says nothing
  about whether it spiked five minutes ago and recovered — continuous collection into a time-series system is
  what makes these metrics actually useful operationally, not individual point queries like this lesson's.
- **Forgetting that JMX metrics reset on broker restart.** A dashboard showing a sudden drop to zero in
  `MessagesInPerSec` right after a rolling restart isn't a real traffic drop — it's the metric's own counter
  starting over, exactly as this lesson's before/after example demonstrated directly.
- **Running with `jmxremote.authenticate=false` outside of local development**, as this lab does for
  simplicity. Unauthenticated remote JMX access on a reachable network interface is a real, serious exposure —
  production brokers need JMX authentication or should keep remote JMX off the network entirely.

## Exercise

Query `UnderReplicatedPartitions`, `RequestHandlerAvgIdlePercent`, and `BrokerTopicMetrics,name=MessagesInPerSec`
against a real broker you control (local or otherwise), once at rest and once immediately after running a
producer benchmark against it. Confirm the `Count` and rate fields change in the direction you'd expect, and
note how quickly the `OneMinuteRate` catches up to reflect a sudden burst compared to the `MeanRate`.
