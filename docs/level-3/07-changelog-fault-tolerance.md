---
description: "A real SIGKILL crash and restart of a Streams app, proving its count recovered correctly from a changelog topic rather than resetting to zero."
---

# 07 · Changelog Topics & Fault Tolerance

Lessons 4 and 5 showed state surviving a restart. This lesson makes it rigorous: a running Streams application
is **genuinely killed with `SIGKILL`** mid-stream — no graceful shutdown, no chance to flush anything
deliberately — and restarted to see exactly what survives and what the recovery actually looks like.

## The real crash-and-recover sequence

```text
$ java -cp ... CrashRecoveryDemo &            # app running, application.id=crash-recovery-demo
$ printf "x:1\nx:2\nx:3\ny:1\n" | bin/kafka-console-producer.sh --topic crash-input ...

$ bin/kafka-console-consumer.sh --topic crash-recovery-demo-KSTREAM-AGGREGATE-STATE-STORE-0000000001-changelog \
    --from-beginning --bootstrap-server localhost:9092 --property print.key=true \
    --value-deserializer ...LongDeserializer --max-messages 4
x    3
y    1

$ kill -9 <pid>                                # a real, hard, ungraceful crash

$ # fresh JVM, same application.id, no special recovery steps taken
$ java -cp ... CrashRecoveryDemo &
$ printf "x:4\nx:5\n" | bin/kafka-console-producer.sh --topic crash-input ...

$ bin/kafka-console-consumer.sh --topic crash-recovery-demo-KSTREAM-AGGREGATE-STATE-STORE-0000000001-changelog \
    --from-beginning --bootstrap-server localhost:9092 --property print.key=true \
    --value-deserializer ...LongDeserializer --max-messages 6
x    3
y    1
x    5
```

(Real output from this course's lab broker — the kill was a genuine `SIGKILL`, the restart was a genuinely
fresh JVM process with no special recovery logic of its own.) Before the crash, `x` had reached `3`. After a
hard kill and an ordinary restart, feeding in two more `x` events brought the count straight to `5` — not `2`
(which a from-scratch restart would have produced) and not some other inconsistent value. The recovery was
entirely automatic: nothing in `CrashRecoveryDemo`'s code does anything special for restart handling: it's the
exact same `KafkaStreams` startup logic every time, and the framework's own default recovery process is what
rebuilt the correct prior count before processing the two new events.

## How It Actually Works

On startup, before processing any new input, a Streams task checks whether its local state store already has
a **checkpoint** (a record of which changelog offset it was last consistent with) matching the current
changelog. If local state is missing, stale, or — as in this lesson's test — potentially inconsistent because
the process was killed mid-write, the task **restores** by reading the changelog topic from the appropriate
point and replaying every update into (a fresh or existing) RocksDB instance before resuming normal processing.
This is exactly the same mechanism Lessons 4 and 5 relied on, just exercised deliberately and rigorously here
rather than by an accidental multi-day gap: a `SIGKILL` is specifically chosen because it gives the process
**no chance** to flush anything gracefully, the worst-case failure a real production deployment actually needs
to tolerate (a killed container, an OOM-killed JVM, a hard machine failure).

This is also exactly why the changelog, not the local RocksDB files, is what Lesson 5 called the real source
of truth: RocksDB's on-disk files after a `SIGKILL` could be in literally any state — mid-write, partially
flushed, whatever the OS and RocksDB's own crash-consistency guarantees leave behind — and the framework
doesn't try to carefully repair that. It's simpler and more robust to treat local state as disposable and
rebuild it from the changelog whenever there's doubt, which is precisely the behavior this lesson's clean `3`
→ `5` result (not `2`, not some corrupted intermediate value) demonstrates.

## Common mistakes

- **Assuming a Streams application needs explicit crash-recovery code.** As this lesson shows, the framework
  handles it by default via changelog replay — application code doesn't need its own persistence or recovery
  logic for state the DSL already manages.
- **Testing "fault tolerance" only with a graceful shutdown (`Ctrl-C`, `streams.close()`).** A clean shutdown
  exercises a completely different, cooperative code path; a real `kill -9` test, as this lesson ran, is the
  only way to actually verify behavior under the failure mode that matters most.
- **Assuming local RocksDB state is safe to copy or back up as "the real data."** As established across this
  level, the changelog topic is authoritative; local store files are a rebuildable cache that doesn't need
  (and generally shouldn't be relied on for) its own backup strategy.

## Exercise

Write a small stateful Streams application (a simple count, as this lesson's is enough), run it, produce some
input, and note the current aggregated value by reading its changelog topic directly. Then genuinely
`kill -9` the process — not a graceful shutdown — restart it, produce a few more events, and confirm the
resulting count is correct and continuous, exactly as this lesson's real test showed, rather than reset or
corrupted.
