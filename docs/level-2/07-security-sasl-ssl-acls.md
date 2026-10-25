---
description: "Real SASL authentication (wrong password rejected) and real ACLs enforced on a live broker, including two genuine authorization gotchas this course hit."
---

# 07 · Security: SASL, SSL & ACLs

Kafka's default configuration trusts every client completely — no identity check, no permission check. This
lesson turns both on for real, on a dedicated single-node broker, and runs straight into two genuinely
surprising ACL requirements that only show up once you actually try to lock things down.

## Two separate questions: who are you, and what are you allowed to do

**Authentication** (SASL, or mutual TLS) answers "who is this client, really" — Kafka supports several SASL
mechanisms (`PLAIN`, `SCRAM`, Kerberos/`GSSAPI`, OAuth); this lesson uses `PLAIN` for simplicity, which checks a
username/password pair against a JAAS configuration file. **Authorization** (ACLs, enforced by an
`Authorizer` implementation) answers a completely different question — "now that I know who you are, what are
you allowed to do" — and the two are independent: a client can authenticate perfectly and still be authorized
for nothing at all.

## Worked example: a real authentication failure

```text
$ # alice connects with the WRONG password
$ bin/kafka-topics.sh --list --bootstrap-server localhost:9192 --command-config wrong-client.properties
ERROR [AdminClient clientId=adminclient-1] Connection to node -1 ... failed authentication due to:
  Authentication failed: Invalid username or password
org.apache.kafka.common.errors.SaslAuthenticationException: Authentication failed: Invalid username or password
```

(Real output from a dedicated single-node SASL_PLAINTEXT broker built for this lesson.) The connection is
rejected before any Kafka-level request even gets a chance — authentication happens at the transport layer,
beneath the API Kafka itself exposes.

## Worked example: two real, separate authorization surprises

```text
$ # admin creates a topic, grants alice Describe+Read on it
$ bin/kafka-acls.sh ... --add --allow-principal User:alice --operation Describe --operation Read --topic secure-topic
$ bin/kafka-topics.sh --describe --topic secure-topic ... --command-config alice-client.properties
Error while executing topic command : Topic authorization failed.
```

```text
$ # separately: alice has topic Write granted, tries to produce
$ echo "test-msg" | bin/kafka-console-producer.sh --topic secure-topic ... --producer.config alice-client.properties
org.apache.kafka.common.errors.ClusterAuthorizationException: Cluster authorization failed.
```

```text
$ # after granting Write on the topic, produce succeeds; then a plain consume:
$ bin/kafka-console-consumer.sh --topic secure-topic --group alice-reader ... --consumer.config alice-client.properties
org.apache.kafka.common.errors.GroupAuthorizationException: Not authorized to access group: alice-reader
```

(All real, from the same lesson broker.) Two genuinely separate gotchas appear here, neither obvious from
reading the ACL model in the abstract: `kafka-topics.sh --describe` bundles a **`ListPartitionReassignments`**
call that needs **cluster-level** `Describe`, not just topic-level `Describe` — granting the topic ACL alone
wasn't enough. And consuming **always** needs a **separate `Read` ACL on the consumer group**, in addition to
`Read` on the topic — a real, well-known Kafka operational trap, because the error message names the group,
not the topic, which is the first place people look.

```text
$ bin/kafka-acls.sh ... --add --allow-principal User:alice --operation Read --group alice-reader
$ bin/kafka-console-consumer.sh --topic secure-topic --group alice-reader --from-beginning ...
Processed a total of 1 messages
```

Only once **both** ACLs — topic `Read` and group `Read` — were granted did consuming actually succeed.

## How It Actually Works

Every Kafka API request carries an authenticated **principal** (`User:alice`, established once at connection
time by SASL) and is checked against the configured `Authorizer` before the broker does any work — each check
names a specific **resource type** (`TOPIC`, `GROUP`, `CLUSTER`, `TRANSACTIONAL_ID`) and a specific
**operation** (`READ`, `WRITE`, `DESCRIBE`, ...). The reason consuming needs a group ACL independent of the
topic ACL is that **group membership itself is a protected resource** — joining a consumer group, triggering
rebalances, committing offsets, are all operations on the *group*, separate from reading records off the
*topic*; Kafka's authorization model treats them as genuinely different things you might want to allow or deny
independently (a client might be allowed to read a topic directly without ever joining a shared group, for
instance). The `--describe` surprise is simpler in spirit: a single CLI command can quietly bundle **several**
underlying API calls, each independently authorization-checked, and the user-facing command only succeeds if
every one of them is permitted — the CLI doesn't tell you which sub-call failed, only that "topic authorization
failed" overall.

## Common mistakes

- **Granting only a topic's `Read` ACL and assuming consuming will work.** As this lesson shows directly, a
  missing group ACL fails with an error that names the group, not the topic — read the error's exact resource
  name, don't assume it's always complaining about the topic.
- **Assuming a single CLI command's failure names the one missing permission.** As the `--describe` case
  shows, one command can need several distinct ACLs across different resource types; granting the "obvious"
  one and still failing doesn't mean the ACL system is broken.
- **Running with `allow.everyone.if.no.acl.found=true` "to avoid these headaches"** — effectively disabling
  authorization for every resource nobody has explicitly configured ACLs for, which quietly defeats the whole
  point of enabling an authorizer in the first place.

## Exercise

Build a real SASL_PLAINTEXT broker with the `StandardAuthorizer` enabled (JAAS config, one broker, one
non-admin user), and reproduce this lesson's sequence: connect with a wrong password, connect correctly but
with no ACLs, grant the topic-level ACL, hit the group-ACL wall when consuming, then grant that too and confirm
a message is actually delivered end to end.
