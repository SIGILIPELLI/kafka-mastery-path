---
description: "The classic eager consumer rebalance protocol vs the new KIP-848 protocol, compared on a real broker including real assignment-strategy differences."
---

# 06 · The New Consumer Rebalance Protocol (KIP-848) vs Classic

Every console consumer started in this course so far has printed the same line on startup: *"The consumer
rebalance protocol (KIP-848) is production-ready!"* This lesson actually tries it, and compares it directly
against the classic protocol every earlier lesson has been using without naming it.

## Classic: eager, cooperative-sticky, and now a next generation

Kafka's **classic** consumer group protocol works by having the group coordinator trigger a **rebalance** —
recomputing the entire partition assignment — whenever membership changes (a consumer joins, leaves, or is
considered dead). The original ("eager") version required *every* consumer in the group to stop consuming
during a rebalance, even ones keeping their same partitions — a real "stop the world" cost on any membership
change. **Cooperative-sticky** (a later, still-classic improvement) reduced this by only revoking partitions
that actually need to move, letting unaffected consumers keep working through most rebalances. **KIP-848**
introduces a genuinely new protocol (not just a new assignment strategy within the old one): a different
wire protocol between consumer and broker, with the broker-side group coordinator playing a larger, more
active role in assignment.

## Worked example: a real classic group vs a real KIP-848 group

```text
$ # classic protocol (the default — every earlier lesson used this)
$ bin/kafka-console-consumer.sh --topic demo-topic --group classic-group --bootstrap-server localhost:9092
$ bin/kafka-consumer-groups.sh --describe --group classic-group --bootstrap-server localhost:9092 --state
GROUP          COORDINATOR (ID)     ASSIGNMENT-STRATEGY  STATE    #MEMBERS
classic-group  localhost:9092 (1)   range                Stable   1

$ # new protocol, opted in explicitly
$ bin/kafka-console-consumer.sh --topic demo-topic --group kip848-group --bootstrap-server localhost:9092 \
    --consumer-property group.protocol=consumer
$ bin/kafka-consumer-groups.sh --describe --group kip848-group --bootstrap-server localhost:9092 --state
GROUP          COORDINATOR (ID)     ASSIGNMENT-STRATEGY  STATE    #MEMBERS
kip848-group   localhost:9092 (1)   uniform              Stable   1

$ bin/kafka-consumer-groups.sh --describe --group kip848-group --bootstrap-server localhost:9092
PARTITION  CONSUMER-ID
0          miNVqnueR7awZwRlkgbcNQ
1          miNVqnueR7awZwRlkgbcNQ
2          miNVqnueR7awZwRlkgbcNQ
```

(Real output from this course's lab broker.) The `ASSIGNMENT-STRATEGY` column is the clearest, most direct
real evidence of the difference: `range` for the classic group (the classic default strategy), **`uniform`**
for the KIP-848 group — a strategy name that doesn't even exist in the classic protocol. The consumer ID itself
looks different too: classic groups generate IDs like `console-consumer-<client-uuid>` (visible in Lesson 5's
output), while this KIP-848 group's member is identified by a short, coordinator-assigned token
(`miNVqnueR7awZwRlkgbcNQ`) instead — a real, visible sign that **the broker's group coordinator**, not the
client, is now the one assigning and tracking member identity.

## How It Actually Works

The core architectural change in KIP-848 is where assignment **decisions** are made. In the classic protocol,
one consumer in the group is elected the "group leader" and is responsible for *computing* the full partition
assignment client-side, then distributing it to the other members through the coordinator — meaning assignment
logic runs inside a consumer application process. In the new protocol, the **broker-side group coordinator**
computes assignments directly, and consumers simply receive their assignment via a lighter-weight heartbeat-
style protocol. This removes an entire class of problems where a slow, buggy, or incompatible client-side
assignor could disrupt a whole group's rebalance — the assignment logic now lives in one place (the broker),
versioned and tested as part of Kafka itself rather than as a pluggable client-side strategy every client
library has to reimplement consistently.

The new protocol is also designed for **incremental** membership changes by default, in a stronger sense than
cooperative-sticky: rather than recomputing from scratch and figuring out a minimal diff, the coordinator tracks
assignments more surgically, aiming to disturb as few existing partition-consumer pairings as possible on any
single membership change — the direct architectural descendant of the problem cooperative-sticky was already
trying to solve within the limits of the older protocol's design.

## Common mistakes

- **Assuming `group.protocol=consumer` is the default and forgetting to check.** As shown above, it must be
  requested explicitly (per-consumer, via this configuration) even on a broker version that fully supports it —
  existing applications keep using the classic protocol until they opt in.
- **Mixing classic-protocol and KIP-848-protocol consumers in the same logical application and expecting them
  to form one group transparently.** Treat a protocol switch as a deliberate, coordinated rollout decision
  across every consumer instance of a given group, not a per-instance toggle to flip casually.
- **Judging the new protocol's benefit only by watching `--describe` output on a quiet, single-consumer group**
  (as this lesson's minimal example necessarily does). The real win — far fewer disrupted partitions during
  membership churn — only shows up under the kind of frequent join/leave activity Lesson 5's 2-to-3-to-2 scenario
  demonstrates; repeating that exact scenario with `group.protocol=consumer` is the natural next experiment.

## Exercise

Repeat Lesson 5's 2-to-3-to-2 consumer group experiment, but start every consumer with
`--consumer-property group.protocol=consumer`. Compare the `kafka-consumer-groups.sh --describe --state` output
(assignment strategy, member ID format) against Lesson 5's classic-protocol run, and note whether partition
reassignment during the 3-to-2 shrink looks any different in how many partitions actually moved.
