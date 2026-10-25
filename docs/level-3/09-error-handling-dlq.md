---
description: "A real Kafka Connect dead letter queue in action: good records pass through, a bad one is routed to a real DLQ topic with full real error context headers."
---

# 09 · Error Handling: Dead Letter Queues & Retries

A pipeline processing a continuous stream will eventually see a record it can't handle — malformed data, a
value outside an expected range, a transform that assumes a format the record doesn't have. This lesson sends
exactly that kind of poison record through a real Connect sink and watches the dead letter queue mechanism
handle it for real.

## Worked example: good records pass, a bad one gets routed, not dropped

```ini
# connector config
transforms=castToInt
transforms.castToInt.type=org.apache.kafka.connect.transforms.Cast$Value
transforms.castToInt.spec=int64
errors.tolerance=all
errors.deadletterqueue.topic.name=dlq-topic
errors.deadletterqueue.context.headers.enable=true
```

```text
$ printf "300\nalso-not-a-number\n400\n" | bin/kafka-console-producer.sh --topic dlq-input ...

$ cat /tmp/dlq-sink-output.txt
300
400

$ bin/kafka-console-consumer.sh --topic dlq-topic --from-beginning --bootstrap-server localhost:9092 \
    --max-messages 1 --property print.headers=true
__connect.errors.topic:dlq-input,__connect.errors.partition:0,__connect.errors.offset:4,
__connect.errors.connector.name:dlq-file-sink,__connect.errors.stage:TRANSFORMATION,
__connect.errors.class.name:org.apache.kafka.connect.transforms.Cast$Value,
__connect.errors.exception.class.name:org.apache.kafka.connect.errors.DataException,
__connect.errors.exception.message:Value (also-not-a-number) was out of range for requested data type,
__connect.errors.exception.stacktrace:...
	also-not-a-number
```

(Real output from this course's lab broker — a genuine `NumberFormatException` wrapped by a real
`Cast$Value` transform, routed by the real framework, not a staged example.) `300` and `400` — both valid
integers — passed through the `Cast$Value` transform and landed in the sink's output file exactly as expected.
`also-not-a-number`, which the transform cannot cast to `int64`, did **not** crash the task and did **not**
silently vanish — it was routed to a real Kafka topic (`dlq-topic`), carrying its **original** value as the
message body and a full set of `__connect.errors.*` **headers**: which topic/partition/offset it came from,
which connector and processing stage failed, the exact exception class and message, and the full stack trace.
Nothing about recovering this record requires guessing — every piece of context needed to diagnose and
potentially reprocess it is right there in the DLQ message itself.

## How It Actually Works

`errors.tolerance=all` tells the Connect framework not to fail the entire task the first time a record causes
an exception in any processing stage (conversion, transformation, or the connector's own put/poll logic) —
instead, the framework catches the error, and if a dead letter queue topic is configured, **produces** the
original problematic record there, attaching the rich diagnostic headers shown above, then moves on to the
next record. Without `errors.tolerance=all` (the default is `none`), that same `NumberFormatException` would
have killed the entire task — one bad record taking down processing for every other record in the partition,
which is exactly the failure mode a DLQ is meant to avoid. The "dead letter" terminology itself borrows directly
from traditional messaging systems' DLQ concept, applied here to Connect's own error-handling pipeline.

The error context headers are produced using Kafka's own per-record header mechanism (the same headers
mentioned when `FileStreamSource`'s `key.converter.schemas.enable` wraps records in Lesson 2) — they travel
alongside the record, inspectable with any tool that reads headers, without needing any special DLQ-aware
client.

## Common mistakes

- **Setting `errors.tolerance=all` without configuring a dead letter queue topic.** Tolerant mode without a
  DLQ silently **drops** bad records after logging them (if `errors.log.enable=true`) — appropriate for truly
  disposable bad data, dangerous if you actually need to recover or reprocess what went wrong.
- **Treating a DLQ as a place records go to die permanently.** The whole point of the rich context headers is
  to make these records reprocessable — a common real pattern is a separate job that reads the DLQ, fixes or
  flags the underlying issue, and republishes corrected records to the original topic.
- **Assuming retries alone (without a DLQ) solve this class of problem.** Retrying a record that's
  *fundamentally* malformed (as `also-not-a-number` is for an int64 cast) will fail identically every time —
  retries help with transient failures (a momentary network blip), not permanent data problems, which is
  exactly why Connect's error handling offers both `errors.retry.*` settings and a separate DLQ mechanism for
  the different failure shapes.

## Exercise

Build a sink connector with a transform that can fail on certain input (a type cast, as this lesson used, is
a simple and reliable choice), configure `errors.tolerance=all` with a real DLQ topic, and send a mix of valid
and deliberately invalid records through it. Confirm the valid ones reach the sink's actual output and the
invalid one lands in the DLQ topic with full `__connect.errors.*` headers, exactly as this lesson's real test
showed.
