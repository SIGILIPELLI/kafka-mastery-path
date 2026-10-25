---
description: "Kafka Connect's architecture: source/sink connectors, workers, tasks, and a real look at exactly which connectors a worker can see by default."
---

# 01 · Kafka Connect Architecture: Source & Sink Connectors

Moving data between Kafka and an external system — a database, a file, a search index — doesn't have to mean
writing a custom producer or consumer application. Kafka Connect is a dedicated framework for exactly this,
and understanding its few moving parts makes the next two lessons' hands-on work make sense immediately.

## The pieces: connectors, workers, tasks

A **connector** is a plugin that knows how to talk to one specific external system — `FileStreamSource` reads
lines from a file and produces them to a topic; a JDBC sink connector consumes from a topic and writes rows to
a database. A connector itself doesn't move data directly — it defines and manages one or more **tasks**, the
units that actually do the work, and the connector's `tasks.max` config controls how many run in parallel. A
**worker** is the actual running process (or one of several, in distributed mode) that executes connectors and
tasks; Level 3's next two lessons run standalone and distributed workers respectively.

## Worked example: a real plugin discovery check

```text
$ CLASSPATH="" bin/connect-standalone.sh config/connect-standalone.properties config/connect-file-source.properties
...
Caused by: org.apache.kafka.connect.errors.ConnectException: Failed to find any class that implements
  Connector and which name matches FileStreamSource, available connectors are:
  PluginDesc{klass=class org.apache.kafka.connect.mirror.MirrorCheckpointConnector, ...}
  PluginDesc{klass=class org.apache.kafka.connect.mirror.MirrorHeartbeatConnector, ...}
  PluginDesc{klass=class org.apache.kafka.connect.mirror.MirrorSourceConnector, ...}
```

(Real output from this course's lab broker — this is a genuine, current-version finding, not a staged
failure.) Even though `FileStreamSource` and `FileStreamSink` ship inside the Kafka distribution
(`libs/connect-file-4.4.0.jar`), a plain worker startup **doesn't automatically see them** — only the
MirrorMaker connectors (which ship on the default classpath directly) are visible. The next lesson shows the
real fix and digs into exactly why — but the architectural point worth absorbing here first is that a Connect
**worker has to be told which connector plugins exist**; nothing is implicitly available just because a jar
happens to be sitting in the Kafka installation's `libs/` directory.

## How It Actually Works

Connect workers discover connector plugins through a **plugin path** mechanism (`plugin.path` config, or
simply the worker's Java classpath, as the example above shows) — at startup, a worker scans the configured
locations for classes implementing the `Connector` interface and builds an in-memory registry of what's
available, which is exactly the list the error message above printed. This isolation is deliberate: Connect
was designed so that each connector plugin's dependencies (a specific database driver version, say) don't leak
into or conflict with another connector's dependencies running on the same worker — a real, practical concern
once you're running several different connector types in production, each potentially needing different, even
conflicting, library versions.

Source and sink connectors are structurally symmetric but conceptually opposite: a **source** connector's
tasks produce records *into* Kafka from the external system (reading a file, polling a database table for
changes); a **sink** connector's tasks consume records *from* a Kafka topic and write them *out* to the
external system. Both run inside the same worker process and framework, both get offset tracking (source
connectors track their position in the external system; sink connectors rely on Kafka consumer offsets as
usual), and both benefit from the same framework-provided retry, error-handling, and (in distributed mode)
automatic task rebalancing across available workers.

## Common mistakes

- **Assuming every connector bundled with the Kafka distribution is automatically usable.** As this lesson's
  real error shows, bundled isn't the same as discoverable — the worker's plugin path has to actually include
  it.
- **Running many unrelated connectors on one worker without considering dependency isolation.** While
  Connect's plugin isolation helps, mixing a large number of connector types with heavy, divergent dependencies
  on a single worker process is still a real operational complexity to plan for, not something to ignore.
- **Confusing "connector" with "task."** A single connector configuration can spawn multiple tasks
  (`tasks.max`) running in parallel — restarting or monitoring "the connector" and "a task" are different
  operations once you're running anything beyond the simplest single-task setup.

## Exercise

Start a Connect worker with a plain, unmodified classpath (as this lesson's example did) and read the full
list of "available connectors" in the resulting error message. Then find where `FileStreamSource` and
`FileStreamSink` actually live in your Kafka distribution's `libs/` directory, and reason about why a worker
wouldn't find them without being told where to look — the next lesson shows the real fix.
