---
description: "What changed when Kafka removed ZooKeeper entirely in 4.0, real proof it's gone from this distribution, and how rolling upgrades work."
---

# 05 · Upgrades, Compatibility & the ZooKeeper-to-KRaft Migration

Every broker in this course has run in KRaft mode from the very first lesson — because as of **Kafka 4.0**,
there is no other option. This lesson looks at what that removal actually meant, with direct proof from this
course's own Kafka installation, and at how rolling upgrades work on an ongoing cluster.

!!! note "What was run, and what wasn't"
    This course's cluster was built fresh in KRaft mode from the start — there was never a legacy
    ZooKeeper-based cluster to migrate *from* in this lab, so the actual migration procedure (running a cluster
    in ZK-migration mode, moving metadata ownership from ZooKeeper to a KRaft quorum live) wasn't performed
    here and is described from Kafka's own documentation, current as of when this course was written. What
    **is** real and directly checked below: that this Kafka distribution's installation genuinely contains
    no ZooKeeper tooling at all.

## Worked example: real, direct proof ZooKeeper is gone

```text
$ ls bin/ | grep -i zoo
(no output)
```

(Real output — this course's Kafka 4.4.0 distribution's `bin/` directory contains zero ZooKeeper-related
scripts: no `zookeeper-server-start.sh`, no `zookeeper-shell.sh`, nothing.) Every earlier version of Kafka
shipped these scripts, because starting and administering a companion ZooKeeper ensemble was a required part
of running any cluster. Their complete absence here is the most direct, concrete evidence of what "ZooKeeper
support removed in Kafka 4.0" actually means: not deprecated, not optional — the tooling to run it isn't even
present anymore.

## How It Actually Works

**The historical ZK-to-KRaft migration.**

For clusters upgrading from a ZooKeeper-based Kafka (3.x and earlier) to KRaft, the process — run entirely
online, without a cluster-wide outage — involved several real, distinct phases: first, deploying a KRaft
controller quorum alongside the still-running ZooKeeper ensemble, with brokers running in a dual-write
"migration mode" that kept both metadata stores in sync; then, once the KRaft quorum had fully caught up and
was verified consistent, migrating metadata *ownership* to the KRaft controllers; and finally, once every
broker was confirmed running purely on KRaft metadata, decommissioning the ZooKeeper ensemble entirely. This
staged approach — sync, then cut over, then remove the old system — is the standard pattern for any
live migration between two sources of truth, not something unique to Kafka, and it's exactly why the
migration could be done without downtime despite replacing something as foundational as the cluster's entire
metadata store.

**Rolling upgrades more generally.** Independent of the ZK-to-KRaft transition specifically, **rolling upgrades** — upgrading broker software
version one node at a time while the cluster keeps serving traffic — rely on two separate compatibility
concepts: the **inter-broker protocol version** (how brokers of potentially different software versions talk
to each other during the transition) and **metadata/log format versions** (what features new metadata records
or log segments are allowed to use). The standard safe sequence is: upgrade broker binaries one at a time
without yet bumping the protocol/format version config, confirm the whole cluster is healthy on the new
binaries while still speaking the *old* protocol version, and only then bump the version config cluster-wide —
because once bumped, older brokers can no longer rejoin, making this step intentionally a one-way door.

## Common mistakes

- **Bumping the inter-broker protocol or metadata version before every broker is confirmed upgraded.** As
  covered above, this step is deliberately irreversible — doing it too early locks out any broker still
  running the older binary, turning a routine rolling upgrade into an outage.
- **Assuming "KRaft" and "no ZooKeeper" were introduced at the same time.** KRaft was available, opt-in,
  for several releases before ZooKeeper support was actually *removed* — conflating "KRaft exists" with
  "ZooKeeper is gone" leads to wrong assumptions about which older Kafka versions can run which way.
- **Skipping version-compatibility checks when upgrading client libraries alongside brokers.** Client and
  broker version compatibility is a related but separate concern from the broker-to-broker protocol version
  discussed here — both need checking independently during any upgrade plan.

## Exercise

Run `ls bin/ | grep -i zoo` (or equivalent) against your own Kafka installation and confirm whether ZooKeeper
tooling is present — note the Kafka version if it is, and compare against the 4.0 removal point. Then read
through your own Kafka distribution's upgrade notes for the exact current guidance on `inter.broker.protocol.
version` sequencing, since this detail is actively maintained and worth checking fresh rather than trusting a
remembered procedure.
