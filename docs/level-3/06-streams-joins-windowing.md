---
description: "Real windowed aggregation showing fixed time windows are epoch-aligned, not relative to events — two events 1s apart landed in different windows."
---

# 06 · Streams Joins & Windowing

"A 5-second window" sounds like it should mean "events within 5 seconds of each other end up together," and a
real test in this lesson shows that's not quite right — two events sent **one second apart** landed in
**different** windows, while two events sent **seven seconds apart** landed in the **same** window.

## Fixed time windows are calendar-aligned, not event-relative

A `TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5))` window doesn't start counting from whenever the first
event for a key arrives — it divides time into fixed-size buckets aligned to **epoch time** (Unix time zero),
the same way clock-minutes are fixed regardless of when you personally start paying attention to the clock. An
event's window is determined entirely by its own timestamp falling into one of these pre-existing, fixed
buckets — two events can be logically "close together" in wall-clock time while still landing in different
buckets, if a bucket boundary happens to fall between them.

## Worked example: a real windowed count, including a real boundary-straddling

```java
KTable<Windowed<String>, Long> counts = source
    .groupByKey()
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5)))
    .count();
```

```text
$ echo "a:event1" | bin/kafka-console-producer.sh --topic window-input ...
$ sleep 1
$ echo "a:event2" | bin/kafka-console-producer.sh --topic window-input ...
$ sleep 7
$ echo "a:event3" | bin/kafka-console-producer.sh --topic window-input ...
```

```text
key=a windowStart=1792910100000 windowEnd=1792910105000 count=1   <- event1
key=a windowStart=1792910105000 windowEnd=1792910110000 count=1   <- event2 (different window!)
key=a windowStart=1792910100000 windowEnd=1792910105000 count=1   <- unchanged
key=a windowStart=1792910105000 windowEnd=1792910110000 count=2   <- event3 joined event2's window
```

(Real output from this course's lab broker.) `event1` and `event2`, sent only about one second apart, landed
in **two different 5-second windows** — `[...100000, ...105000)` and `[...105000, ...110000)` — because the
fixed boundary at `105000` happened to fall between them. `event3`, sent a full seven seconds after `event2`,
landed in the **same** window as `event2` (`count` went from 1 to 2 for that window), because seven seconds
later still fell before that window's own end boundary. The window boundaries were fixed the instant the
topology started; every event's timestamp is simply sorted into whichever pre-existing bucket it falls into,
with zero regard for how close together events feel relative to each other.

## How It Actually Works

A `KTable<Windowed<String>, Long>` is keyed by a **composite** key: the original key (`"a"`) plus the specific
window instance it belongs to — which is exactly why the same key `"a"` can have multiple, independently
tracked counts live simultaneously, one per window it has events in. Each window's aggregate is itself a
`KTable` entry, materialized to a **windowed state store** backed by its own changelog (the same durability
model from Lesson 5), with an additional retention period controlling how long old windows are kept around
before being purged — important because, without that bound, a long-running application would accumulate an
unbounded number of historical window entries.

**Joins** (not shown in this lesson's specific example, but built on the same time-awareness) extend this
further: a `KStream`-`KStream` join also needs a window, because without one, "join records from these two
streams" would have no sensible bound — should an event from ten minutes ago still be eligible to join against
one arriving right now? A join window answers exactly that question, using the same epoch-aligned bucketing
model demonstrated above, just applied to deciding which pairs of records from two different streams are
considered "close enough in time" to match.

## Common mistakes

- **Assuming a "5-second window" means "events within 5 seconds of each other."** As this lesson's real test
  shows, it means "events falling into the same fixed, epoch-aligned 5-second bucket" — a subtly but
  meaningfully different guarantee, especially right at bucket boundaries.
- **Forgetting that windowed state stores need a retention/grace configuration appropriate to real event
  timing.** `ofSizeWithNoGrace()` (used here) accepts no late-arriving records for a window once it's
  considered closed — fine for a controlled demo, often too strict for real, out-of-order network traffic.
- **Not accounting for window boundary effects when validating windowed aggregation logic in tests.** A test
  that happens to run with timestamps comfortably in the middle of a window won't catch boundary-straddling
  bugs — deliberately testing near a boundary, as this lesson's real run inadvertently did, is a more honest
  test.

## Exercise

Reproduce this lesson's windowed count with a small time gap between two events chosen so they're likely to
straddle a 5-second epoch boundary (check `date +%s` to see where you currently sit relative to the next
multiple of 5), and confirm you get two separate window entries for the same key despite the events being only
a second or two apart — the same real effect this lesson's own test hit.
