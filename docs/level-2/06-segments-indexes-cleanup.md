---
description: "Watching real segments roll at a byte boundary, then watching real retention delete all of them with a genuine .log.deleted rename step."
---

# 06 · Log Segments, Indexes & Cleanup in Practice

Level 1 introduced segments and showed one. This lesson watches segments roll in real time as data is written,
then watches time-based retention genuinely delete them — including a real, two-step deletion process this
course's own test run surfaced along the way.

## Worked example: real segment rolling at a byte boundary

```text
$ bin/kafka-topics.sh --create --topic segment-demo --bootstrap-server localhost:9092 \
    --config segment.bytes=1048576 --config retention.ms=5000 --config segment.ms=2000
$ bin/kafka-producer-perf-test.sh --topic segment-demo --num-records 20000 --record-size 200 ...

$ ls -la .../segment-demo-0/*.log
1048128  00000000000000000000.log
1048128  00000000000000004992.log
1048128  00000000000000009984.log
1048128  00000000000000014976.log
   6749  00000000000000019968.log   <- active segment, still growing
```

(Real output from this course's lab broker.) With `segment.bytes=1048576` (1 MB), the broker rolled a new
segment **exactly** at that size, four times in a row, producing base offsets `0`, `4992`, `9984`, `14976`,
`19968` — each roughly 4,992 records apart, consistent with 200-byte records plus overhead fitting almost
exactly 4,992 times into 1 MB. The final, smaller segment is the **active** one, still being appended to.

## Worked example: real retention deletion, including a genuine two-step process

```text
$ # (broker log, after retention.ms=5000 elapsed and the check interval ran)
INFO [UnifiedLog partition=segment-demo-0] Deleting segment LogSegment(baseOffset=0, ...)
  due to log retention time 5000ms breach based on the largest record timestamp in the segment
INFO [UnifiedLog partition=segment-demo-0] Deleting segment LogSegment(baseOffset=4992, ...) due to ...
INFO [UnifiedLog partition=segment-demo-0] Deleting segment LogSegment(baseOffset=9984, ...) due to ...
INFO [UnifiedLog partition=segment-demo-0] Deleting segment LogSegment(baseOffset=14976, ...) due to ...
INFO [UnifiedLog partition=segment-demo-0] Deleting segment LogSegment(baseOffset=19968, ...) due to ...
INFO [UnifiedLog partition=segment-demo-0] Incremented log start offset to 20000 due to segment deletion

$ ls -la .../segment-demo-0/
1048128  00000000000000000000.log.deleted
1048128  00000000000000004992.log.deleted
1048128  00000000000000009984.log.deleted
1048128  00000000000000014976.log.deleted
   6749  00000000000000019968.log.deleted
      0  00000000000000020000.log

$ bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic segment-demo --time -2
segment-demo:0:20000
$ bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic segment-demo --time -1
segment-demo:0:20000
```

(Real output — this topic genuinely had all 20,000 records retained for only 5 seconds, by design, to make
this lesson's point observable quickly.) **Every** segment got deleted, including the previously-active one —
once every record in the log, even the newest, is older than `retention.ms`, there's nothing left to keep.
Notice the files weren't simply removed: they were **renamed to `.log.deleted`** first, and a brand-new, empty
active segment (`00000000000000020000.log`) was created. Both the earliest and latest offset now report
`20000` — the log is genuinely, completely empty of the old data, even though the renamed `.deleted` files
were still sitting on disk at the moment this was checked.

## How It Actually Works

The periodic log-cleanup thread runs every `log.retention.check.interval.ms` (the default is a relatively
relaxed **5 minutes** — this lesson lowered it to 2 seconds specifically to make the demonstration observable
in a reasonable time, and a short `retention.ms` **without** also lowering the check interval would mean data
technically eligible for deletion sits around for up to 5 minutes longer than you might expect, a real and
common point of confusion). Each closed or active segment's eligibility is judged by its **largest record
timestamp** compared against `retention.ms` — once every segment in a partition breaches that threshold, the
log's start offset advances past all of it.

Deletion itself is **two-phase**, exactly as observed above: a segment marked for deletion is first renamed
with a `.deleted` suffix (removing it from the broker's active, in-memory view of the log immediately, so reads
and writes no longer see it) and only **physically unlinked from disk** after `log.segment.delete.delay.ms`
(default 60 seconds) passes. This delay exists to protect against a narrow race: a reader that had already
opened a file handle to a segment just before it was logically removed shouldn't have the underlying file
yanked out from under it mid-read; the renamed `.deleted` file sticks around briefly so any in-flight read
can finish safely before the bytes are actually reclaimed.

## Common mistakes

- **Setting an aggressively short `retention.ms` and expecting near-instant cleanup**, without checking
  `log.retention.check.interval.ms`. As this lesson shows, the check interval's default (5 minutes) can make a
  5-second retention setting behave like a 5-minute one in practice unless the interval is also tuned.
- **Seeing `.log.deleted` files during a disk-space investigation and assuming something is stuck or broken.**
  They're expected, temporary artifacts of the two-phase deletion process — they'll disappear on their own
  after `log.segment.delete.delay.ms`, and lowering that value (with caution) is the right lever if disk
  pressure from this delay window is a genuine operational concern.
- **Assuming the active segment is exempt from retention deletion.** As this lesson's real log output shows
  directly, if the data in it is old enough, it gets rolled and deleted along with everything else — there's no
  special protection for "the segment currently being written to" beyond it naturally containing the newest
  data, which is usually (but not always, as shown here) younger than the retention threshold.

## Exercise

Create a topic with a short `retention.ms` and a short `log.retention.check.interval.ms` (you'll need to set
the latter at the broker config level, as this lesson did, since it isn't a per-topic setting), produce enough
data to roll at least two segments, and watch the broker log for the real deletion messages. Confirm the
`.log.deleted` rename step yourself with `ls`, and check again after `log.segment.delete.delay.ms` passes to
confirm the files are finally gone.
