---
description: "How a Kafka partition is really stored on disk: segment files, the offset/time index, and dumping a real log segment to see raw records."
---

# 03 · Topics & Partitions: How the Log Is Really Stored

Lesson 1 described a partition as "an append-only log file." This lesson looks at what that actually means on
disk — not metaphorically, but by opening the real files a partition is made of and dumping their contents with
Kafka's own tooling.

## What's really in a partition's directory

Each partition lives in its own directory, named `<topic>-<partition>` (so `demo-topic-0`, `demo-topic-1`,
`demo-topic-2` for Lesson 1's three-partition topic). Inside, the log isn't one single ever-growing file —
it's split into **segments**, each a trio of files sharing a base-offset filename: a `.log` file (the actual
records), a `.index` file (an offset-to-byte-position index, so Kafka doesn't have to scan the whole log
file to find a given offset), and a `.timeindex` file (a timestamp-to-offset index, used for time-based
lookups like "give me everything after this timestamp").

## Worked example: a real partition directory and a real log dump

```text
$ ls -la /tmp/kraft-combined-logs/demo-topic-0/
-rw-r--r--  10485760 00000000000000000000.index
-rw-r--r--       100 00000000000000000000.log
-rw-r--r--  10485756 00000000000000000000.timeindex
-rw-r--r--         8 leader-epoch-checkpoint
-rw-r--r--        43 partition.metadata

$ bin/kafka-dump-log.sh --files /tmp/kraft-combined-logs/demo-topic-0/00000000000000000000.log --print-data-log
Dumping /tmp/kraft-combined-logs/demo-topic-0/00000000000000000000.log
Log starting offset: 0
baseOffset: 0 lastOffset: 2 count: 3 baseSequence: 0 lastSequence: 2 producerId: 0 producerEpoch: 0
  partitionLeaderEpoch: 0 isTransactional: false isControl: false position: 0 CreateTime: 1792846095985
  size: 100 magic: 2 compresscodec: none crc: 3648714602 isvalid: true
| offset: 0 CreateTime: 1792846095977 keySize: -1 valueSize: 6 sequence: 0 headerKeys: [] payload: hello1
| offset: 1 CreateTime: 1792846095985 keySize: -1 valueSize: 6 sequence: 1 headerKeys: [] payload: hello2
| offset: 2 CreateTime: 1792846095985 keySize: -1 valueSize: 6 sequence: 2 headerKeys: [] payload: hello3
```

(Real output from this course's lab broker — these are the exact three messages produced in Lesson 1's
worked example, read back directly from the raw file on disk, not through any Kafka client API.) The filename
`00000000000000000000.log` is the segment's **base offset** (zero, padded) — the offset of the first record it
contains; a broker rolls over to a new segment file (named with the next base offset) once the current one
hits a size or time limit (Lesson 9 covers this). The index files are large (over 10 MB each) despite this tiny
segment holding only 3 records and 100 bytes — Kafka **pre-allocates** index file space up front
(`log.index.size.max.bytes`) rather than growing it incrementally, trading some wasted disk space for avoiding
repeated file-resize operations under load. `kafka-dump-log.sh` reads the segment's binary format directly and
shows exactly what's really there: each record's own offset, creation timestamp, key/value sizes (`keySize: -1`
here means no key was set — Lesson 4 covers what changes when one is), and the raw payload bytes.

## How It Actually Works

The reason Kafka indexes by **byte position**, not by scanning records one at a time, is pure performance:
fetching a specific offset or a specific timestamp without an index would mean reading and deserializing every
record from the start of the segment until the target is found — fine for a tiny demo segment, ruinous for a
segment holding gigabytes. The `.index` file maps a sparse set of offsets to the exact byte position in the
`.log` file where that offset's record begins, letting a fetch jump almost directly to the right spot and scan
only a small amount from there. This is also why the dump above shows `position: 0` for the batch starting at
offset 0 — that's the literal byte offset within the `.log` file, the same number the `.index` file would store
to let a future fetch skip straight there.

Records in modern Kafka are written in **batches** (the `baseOffset`/`lastOffset`/`count` line above describes
one batch containing all three records, even though they were produced one at a time by the console producer
tool) rather than one record per on-disk write. Batching is what makes Kafka's throughput possible at all:
producers buffer multiple records client-side and send them as a single batch (Lesson 4 covers the exact
configuration that controls this), and the broker writes and replicates that whole batch as one unit — far
fewer, larger I/O operations than one-record-at-a-time would allow, at the cost of a small, tunable delay while
records accumulate into a batch.

## Common mistakes

- **Assuming more partitions always means more index/segment overhead that scales badly.** Pre-allocated index
  files are per-segment, not per-record, so the fixed overhead is bounded and doesn't grow with how many
  messages a segment holds — the real cost of more partitions is elsewhere (more file handles, more replication
  traffic, more metadata), not index bloat.
- **Trying to read or edit `.log` files directly with a text editor "to see what's in them."** They're a binary
  format with length-prefixed records and batch headers, not plain text — `kafka-dump-log.sh` exists
  specifically because the format isn't meant to be read any other way.
- **Confusing a partition's offset with a byte position in the file.** They're related through the index but
  are not the same number — offset 2 is the third record *logically*, wherever it happens to physically sit in
  the `.log` file (`position: 0` in this example because all three records landed in one batch starting at the
  very beginning of the segment).

## Exercise

Produce enough messages to one partition that you can find more than one segment file under its directory (you
may need to lower `log.segment.bytes` significantly for a quick demo — `log.segment.bytes=1024` creates new
segments almost immediately). Run `kafka-dump-log.sh --files <path> --print-data-log` against each segment file
you find, and confirm each one's `baseOffset` picks up exactly where the previous segment's `lastOffset` left
off.
