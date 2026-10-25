---
description: "A real Kafka Connect standalone worker, the exact classpath fix for the FileStreamSource plugin-discovery gap, and real data landing in a topic."
---

# 02 · Running Connect Standalone

Lesson 1 hit a real wall: a plain Connect worker couldn't find the `FileStreamSource` connector at all, despite
it shipping inside the Kafka distribution. This lesson finds and applies the real fix, and proves data actually
flows end to end.

## The real fix

```text
$ CLASSPATH="libs/connect-file-4.4.0.jar" bin/connect-standalone.sh \
    config/connect-standalone.properties connect-file-source.properties
...
INFO Added plugin 'org.apache.kafka.connect.file.FileStreamSourceConnector' (DelegatingClassLoader)
INFO Added alias 'FileStreamSource' to plugin 'org.apache.kafka.connect.file.FileStreamSourceConnector'
...
INFO Created connector local-file-source (ConnectStandalone)
```

(Real output from this course's lab broker.) Explicitly putting `connect-file-4.4.0.jar` on the worker's
`CLASSPATH` is all it took — the worker's plugin scanner found `FileStreamSourceConnector` this time, registered
it under both its full class name and the short alias `FileStreamSource` the config file actually references,
and successfully created the connector instance. The alternative, more production-appropriate fix is setting
`plugin.path` in the worker's properties file to a directory containing the connector jar (or a subdirectory
per connector) — `CLASSPATH` works for a quick local test, `plugin.path` is the mechanism meant for managing
multiple connector plugins cleanly in a real deployment.

## Worked example: data actually flowing, source file to topic

```ini
# connect-file-source.properties
name=local-file-source
connector.class=FileStreamSource
tasks.max=1
file=/tmp/connect-test-input2.txt
topic=connect-test2
```

```text
$ printf "line1\nline2\nline3\n" > /tmp/connect-test-input2.txt
$ CLASSPATH="libs/connect-file-4.4.0.jar" bin/connect-standalone.sh ...

$ bin/kafka-console-consumer.sh --topic connect-test2 --from-beginning --bootstrap-server localhost:9092 --max-messages 3
{"schema":{"type":"string","optional":false},"payload":"line1"}
{"schema":{"type":"string","optional":false},"payload":"line2"}
{"schema":{"type":"string","optional":false},"payload":"line3"}
```

(Real output — the connector genuinely read the three lines from a real file on disk and produced them to a
real topic.) Each line arrived wrapped in a JSON envelope carrying a `schema` alongside the `payload` — this
is the worker's configured `JsonConverter` with `schemas.enable=true` doing its job: every record flowing
through Connect gets this schema/payload structure applied by the converter, independent of whatever format
the connector itself produces internally.

## How It Actually Works

**Standalone mode** runs exactly one worker process, tracking connector and task configuration *and* offsets in
local files (`offset.storage.file.filename`, visible in the worker's own config dump as `/tmp/connect.offsets`)
rather than in Kafka topics — fine for a single machine, local development, or a connector that genuinely only
ever needs to run once, but with an obvious single point of failure: if this one process dies, nothing is
running the connector until it's restarted, and if the local offset file is lost, the connector doesn't know
where it left off. This is precisely the limitation **distributed mode** (next lesson) removes — storing
offsets and configuration in Kafka itself, shared across multiple worker processes, so the framework can
reschedule work onto a surviving worker if one goes down.

The `JsonConverter`'s schema wrapping is also worth understanding structurally: Connect's internal data model
represents every record as a `(schema, value)` pair regardless of source, and `JsonConverter` serializes both
parts when `schemas.enable=true` — this is what lets a sink connector on the other end reconstruct a typed
value rather than just raw bytes, at the cost of visibly larger messages (as seen above) compared to a plain
string producer.

## Common mistakes

- **Assuming a connector shipped with Kafka is automatically usable without a `plugin.path` or `CLASSPATH`
  addition.** As this lesson (and Lesson 1) show directly, it isn't — always verify the worker's startup log
  actually lists the plugin you expect before trying to create a connector that uses it.
- **Using standalone mode for anything beyond local testing or a genuinely single-instance job.** The
  local-file offset storage and single-process execution model don't tolerate the worker process dying the way
  distributed mode does.
- **Not noticing the JSON schema-wrapping overhead when `schemas.enable=true`.** For high-volume pipelines,
  this per-record overhead is real and worth weighing against simpler converters (`StringConverter`,
  `ByteArrayConverter`) when the extra structure isn't actually needed downstream.

## Exercise

Reproduce this lesson's exact fix: start a Connect standalone worker with a plain classpath, confirm the
`FileStreamSource` discovery failure from Lesson 1, then add `libs/connect-file-<version>.jar` to `CLASSPATH`
(or configure `plugin.path` instead) and confirm the connector now loads and real data from a file you create
ends up in the target topic, exactly as shown here.
