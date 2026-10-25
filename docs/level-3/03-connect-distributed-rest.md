---
description: "A real Kafka Connect distributed worker managed entirely through its REST API — creating, checking status, and deleting a connector with curl."
---

# 03 · Running Connect Distributed & the REST API

Standalone mode (Lesson 2) manages connectors through a static config file read once at startup. Distributed
mode manages them live, over HTTP, which is what lets you add, check, and remove connectors on a running
cluster without ever restarting a worker process — demonstrated here entirely with real `curl` calls.

## Worked example: a connector's full lifecycle over REST

```text
$ curl -X POST http://localhost:8084/connectors -H "Content-Type: application/json" -d '{
  "name": "dist-file-source",
  "config": {
    "connector.class": "FileStreamSource",
    "tasks.max": "1",
    "file": "/tmp/connect-dist-input.txt",
    "topic": "connect-dist-test"
  }
}'
{"name":"dist-file-source","config":{...},"tasks":[],"type":"source"}

$ curl http://localhost:8084/connectors/dist-file-source/status
{"name":"dist-file-source","connector":{"state":"RUNNING","worker_id":"127.0.0.1:8084","version":"4.4.0"},
 "tasks":[{"id":0,"state":"RUNNING","worker_id":"127.0.0.1:8084","version":"4.4.0"}],"type":"source"}

$ bin/kafka-console-consumer.sh --topic connect-dist-test --from-beginning --bootstrap-server localhost:9092 --max-messages 2
"distline1"
"distline2"

$ curl -X DELETE http://localhost:8084/connectors/dist-file-source -w "\nhttp_code=%{http_code}\n"
http_code=204

$ curl http://localhost:8084/connectors
[]
```

(Real output from a real distributed worker on this course's lab broker.) No properties file, no restart — a
plain HTTP `POST` created the connector, its own task immediately reported `RUNNING`, real data from the
configured file landed in the topic within a couple of seconds, and a `DELETE` removed it cleanly (HTTP `204`,
and the connector list is empty again afterward). Every one of these operations happened against one already-
running worker process.

## How It Actually Works

Distributed mode's defining feature is that connector **configuration, status, and offsets** are stored in
three dedicated Kafka topics (`connect-configs`, `connect-status`, `connect-offsets` in this lesson's setup,
each visible being started as a `KafkaBasedLog` in the worker's own startup log) instead of local files. This
is what makes the REST API meaningful across a *cluster* of workers rather than just one: a `POST` to any
worker's REST endpoint writes the new connector config to the shared `connect-configs` topic, every worker in
the group consumes that topic and reacts, and the group's internal rebalance protocol (visible in this
lesson's startup log as `"Rebalance started"` — distributed Connect workers form a consumer-group-like
coordination group, conceptually similar to Level 1's consumer groups, just coordinating task assignment rather
than partition assignment) decides which worker actually runs which task. If a worker dies, the remaining
workers detect it through the same mechanism and reassign its tasks — the exact resilience standalone mode
can't offer, because standalone has no group to rebalance into.

The REST API itself is just a thin HTTP layer over this same shared state: creating a connector is really
"write this config to the config topic and let the group's rebalance protocol pick it up," and checking status
is "read the latest status record workers have written to the status topic" — which is why status can lag
reality very slightly, and why a connector's status genuinely reflects what a worker reported, not a live,
synchronous query of the running task.

## Common mistakes

- **Managing distributed-mode connectors by editing a local properties file**, the standalone-mode habit —
  distributed mode has no such per-connector file; the REST API (or a config-management tool that wraps it) is
  the only supported way to create or change connectors once workers are running.
- **Assuming a `201`/`200`-style success response from the REST API** rather than checking the actual
  documented codes — this lesson's delete correctly returned `204` (No Content), not `200`, which is easy to
  mis-check in scripted automation that only tests for one specific success code.
- **Treating `/connectors/<name>/status` as instantaneous ground truth.** As explained above, it reflects the
  latest status record written to the internal topic — briefly stale information after a very recent change is
  expected, not a bug.

## Exercise

Start a distributed worker, create a connector purely through `curl` POST requests (no properties file),
verify its status reports `RUNNING` and that real data actually appears in the target topic, then delete it
and confirm the connector list is empty again — reproducing this lesson's full, real lifecycle yourself.
