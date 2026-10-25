---
description: "What a KRaft controller quorum actually tolerates: a real majority-preserving failure, and a real majority-losing one that genuinely hangs."
---

# 01 · Multi-Broker Clusters & the KRaft Quorum in Practice

Level 1 introduced the KRaft controller quorum and stated that it needs a majority of voters to make progress.
This lesson proves it two ways on a real 3-node cluster: once by losing a minority (and watching everything
keep working), and once by losing a majority (and watching a real metadata operation genuinely hang).

## Worked example: losing one voter out of three — no problem

```text
$ kill <node-1-pid>          # 1 of 3 controller voters down

$ bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9094 describe --status
LeaderId:        2
CurrentVoters:   [{"id": 1, ...}, {"id": 2, ...}, {"id": 3, ...}]

$ bin/kafka-topics.sh --create --topic quorum-test --bootstrap-server localhost:9094 \
    --partitions 1 --replication-factor 1
Created topic quorum-test.
```

(Real output from this course's lab cluster.) With node 1 down, **2 of 3** voters are still reachable — still a
majority. The quorum leader (node 2, unaffected since it wasn't the one killed) kept working, and a real
metadata operation (creating a topic, which requires committing a change through the controller quorum)
succeeded immediately, with no visible disruption at all.

## Worked example: losing a second voter — a real, genuine hang

```text
$ kill <node-3-pid>          # now 2 of 3 controller voters down; only node 2 remains

$ bin/kafka-topics.sh --create --topic quorum-test2 --bootstrap-server localhost:9094 \
    --partitions 1 --replication-factor 1
(hangs — no response after 8+ seconds)
```

(Real output — this command genuinely did not return.) With only **1 of 3** voters reachable, there is no
majority left to commit anything through Raft consensus. The create request isn't rejected with a clean error
— it simply has nowhere to make progress, because committing a change requires a majority of voters to
acknowledge it, and that majority doesn't exist anymore. Restarting the two killed nodes restored the quorum to
3/3, and the pending request (which the client was still holding/retrying) completed once majority was
available again — a real illustration that a well-behaved client can be *patient* through a quorum outage
rather than failing outright, which is a double-edged property worth knowing about operationally: your
tooling may look "stuck" rather than clearly "erroring," and that's expected, not a bug in the tool.

## How It Actually Works

Raft-based consensus (which KRaft uses directly, Level 1 Lesson 2) requires every committed change to be
acknowledged by a **strict majority** of voters before it's considered durable — this is what guarantees that
even if some voters are later lost, the committed decision isn't silently reversed, because any future majority
must necessarily overlap with the majority that committed it. With 3 voters, the majority is 2 — losing 1 still
leaves a majority; losing 2 does not. This is precisely why KRaft (and Raft deployments generally) use **odd**
numbers of voters: an even number doesn't buy you extra fault tolerance over the next-lower odd number (5
voters tolerates 2 failures, exactly like 5; but a 4-voter quorum still only tolerates 1 failure before losing
majority, the same as a 3-voter quorum, while needlessly costing a 4th node's worth of resources and network
overhead for no additional resilience).

This is also why the quorum describe command kept answering during the minority-loss case — **reads** of the
current known state can often still be served by a quorum that's lost some members, as long as a leader with a
view of the committed log is reachable; it's specifically *committing new changes* that strictly requires a
live majority.

## Common mistakes

- **Treating a hung metadata operation as "the cluster is broken" rather than checking quorum health first.**
  As this lesson shows directly, a hang with no error is the expected signature of a lost controller majority —
  `kafka-metadata-quorum.sh describe --status` against any surviving broker is the first diagnostic step, not a
  broker restart.
- **Sizing a controller quorum at an even number "for extra safety."** As explained above, an even-sized
  quorum doesn't improve fault tolerance over the next odd number down — it only adds cost and complexity.
- **Assuming killing a controller voter immediately breaks client-facing broker traffic.** As this lesson's
  first test shows, losing a controller-quorum minority doesn't stop existing partitions from serving
  produce/fetch traffic at all — only *new metadata changes* (new topics, partition reassignments, ACL updates)
  are blocked once majority is lost, not already-established data-plane operations.

## Exercise

On a 3-node cluster like this lesson's lab, kill one controller voter and confirm (via `kafka-metadata-quorum.sh
describe --status` and a real topic creation) that the cluster still accepts metadata changes. Then kill a
second voter and confirm a metadata operation genuinely hangs rather than failing with a clean error. Restart
both nodes and confirm the cluster recovers and any pending operation completes.
