---
description: "What the Strimzi Kubernetes operator actually automates for running Kafka, and which concerns from earlier levels it does and doesn't remove."
---

# 08 · Kafka on Kubernetes with Strimzi

!!! note "Documentation-based lesson"
    No Kubernetes cluster was available in this lab (no `kubectl`, `minikube`, or `kind` installed), so this
    lesson is built from Strimzi's own current documentation rather than a real deployment. Every concept
    here, though, maps directly onto something this course actually ran manually in earlier lessons — the
    goal is connecting what Strimzi automates to the real mechanics you've already seen with your own hands.

## What an operator actually automates

**Strimzi** is a Kubernetes operator — a controller that watches for custom resources (a `Kafka` resource
describing a desired cluster, a `KafkaTopic` resource describing a desired topic) and continuously reconciles
the real cluster state toward what's declared, the same general pattern every Kubernetes operator follows for
whatever system it manages. Concretely, this means things this course did by hand — formatting storage with
`kafka-storage.sh`, writing `server.properties` by hand for each of the 3 nodes in this lab's cluster,
starting each broker process, creating topics with `kafka-topics.sh --create` — become declarative YAML: you
describe a 3-broker cluster with a given replication factor, and the operator handles provisioning pods,
persistent volumes, generating the equivalent of this course's own hand-written `server.properties` files, and
keeping the running cluster converged on that desired state.

## Mapping Strimzi concepts onto what this course actually ran

A Strimzi `Kafka` custom resource's `replicas` field is the Kubernetes-native equivalent of this course
manually starting three separate `kafka-server-start.sh` processes with three hand-written config files
(Level 1, Lesson 2) — the operator handles pod scheduling and the KRaft quorum wiring Lesson 2 configured
by hand via `controller.quorum.voters`. A `KafkaTopic` custom resource with a `partitions`/`replicas` spec is
the declarative equivalent of this course's many `kafka-topics.sh --create --partitions N --replication-
factor M` commands — except reconciled continuously, so editing the YAML and re-applying it is how you'd
trigger what Level 2, Lesson 8's manual `kafka-reassign-partitions.sh` run did by hand. Strimzi's "Kafka
Connect" custom resource similarly wraps what Level 3's `connect-distributed.sh` plus manual REST calls did —
declaring connector configuration as a `KafkaConnector` resource instead of a `curl POST`.

## How It Actually Works

The operator pattern fundamentally works by **continuous reconciliation**: Strimzi doesn't just run your YAML
once at creation time — it watches the actual state of the cluster (pod health, PVC status, broker
configuration as reported by the Kafka Admin API) against the desired state in your custom resources, and
takes action whenever they drift apart, the same loop every Kubernetes controller runs for every resource
type it manages. This is genuinely powerful for the operational concerns Level 4's earlier lessons covered
manually: a pod crash (the Kubernetes-native equivalent of this course's own `kill -9` broker tests,
Lesson 3) gets automatically rescheduled and rejoined to the cluster by the operator's own reconciliation loop,
without a human running `kafka-server-start.sh` again by hand.

What Strimzi does **not** remove is the underlying Kafka concepts every other lesson in this course covered —
replication factor still determines fault tolerance (Level 1, Lesson 7's real failover test behaves
identically whether the broker was started by hand or by an operator), ISR and under-replication monitoring
(Level 4, Lesson 3) are still the real signals to watch, and partition/replica capacity planning (Level 4,
Lesson 2) still governs how much storage a Strimzi-managed cluster actually needs. The operator automates
*mechanics* — provisioning, config generation, pod lifecycle — not the Kafka-level reasoning this whole course
built.

## Common mistakes

- **Assuming an operator removes the need to understand Kafka's own operational model.** Strimzi automates
  how you *express* a desired cluster state; diagnosing an under-replicated partition or a consumer lag
  problem on a Strimzi-managed cluster still requires exactly the Kafka-level knowledge Levels 1, 2, and this
  level's earlier lessons built, not Kubernetes knowledge alone.
- **Treating pod restarts as equivalent to a clean broker shutdown.** A Kubernetes-scheduled pod eviction
  or crash is, from Kafka's perspective, the same kind of ungraceful failure this course's own `kill -9` tests
  exercised (Level 3, Lesson 7; Level 4, Lesson 3) — the same real recovery mechanics apply, they're just
  triggered by the scheduler instead of a manual kill command.
- **Sizing persistent volumes without the real capacity math from Lesson 2.** A declarative YAML `storage`
  spec is still bound by the same per-record overhead and replication-factor multiplication measured directly
  earlier in this level — the operator doesn't change the arithmetic, only who/what writes the config.

## Exercise

If you have access to a Kubernetes cluster (even a local `kind`/`minikube` one), install Strimzi and deploy a
small Kafka cluster from its example YAML. Create a topic via a `KafkaTopic` resource, then deliberately
delete one broker pod and watch the operator's reconciliation bring it back — compare what you observe against
this course's own manual `kill -9` broker test from Level 4, Lesson 3, and confirm the underlying Kafka-level
recovery (ISR shrink, leader failover, recovery) looks the same either way.
