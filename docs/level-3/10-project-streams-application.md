---
description: "Project: a real, complete Kafka Streams app — filter, map, and stateful count chained together — run end to end with verified, correct real output."
---

# 10 · Project: A Real Kafka Streams Application

Every earlier lesson in this level ran one isolated piece of Streams (a word count, a windowed count, a crash
recovery test). This project chains **filter → map → stateful count** into one real, small analytics
pipeline, and verifies every stage's behavior against real input, not just the final number.

## The pipeline

Input lines look like `category,amount` (an order's category and its value). The pipeline keeps only orders
over $100, re-keys by category, and counts how many high-value orders each category has had:

```java
KTable<String, Long> highValueCountsByCategory = orders
    .filter((key, value) -> {
        double amount = Double.parseDouble(value.split(",")[1]);
        return amount > 100.0;
    })
    .map((key, value) -> KeyValue.pair(value.split(",")[0], value))
    .groupByKey()
    .count();

highValueCountsByCategory.toStream().to("orders-category-counts", Produced.with(Serdes.String(), Serdes.Long()));
```

## Running it against real, mixed data

```text
$ printf "electronics,150\nbooks,20\nelectronics,200\nbooks,500\nclothing,45\nelectronics,80\n" | \
    bin/kafka-console-producer.sh --topic orders-raw --bootstrap-server localhost:9092
```

```text
FILTER: electronics,150 -> KEEP
FILTER: books,20 -> DROP
FILTER: electronics,200 -> KEEP
FILTER: books,500 -> KEEP
FILTER: clothing,45 -> DROP
FILTER: electronics,80 -> DROP
```

```text
$ bin/kafka-console-consumer.sh --topic orders-category-counts --from-beginning --bootstrap-server localhost:9092 \
    --property print.key=true --value-deserializer ...LongDeserializer
electronics   2
books         1
```

(Real output from this course's lab broker — six real orders, through a real three-stage pipeline.) Every
filter decision is exactly right: the three orders at or below $100 (`books,20`, `clothing,45`,
`electronics,80`) were dropped; the three above $100 (`electronics,150`, `electronics,200`, `books,500`) were
kept. The final counts are exactly right too: `electronics` kept 2 of its 3 orders, `books` kept 1 of its 2,
and `clothing` — whose only order was filtered out — **never appears in the output at all**, which is itself
worth noting: a `KTable` only has entries for keys it has actually seen a contributing record for; there's no
"zero count" row materializing for categories the pipeline never kept anything from.

## How It Actually Works

This pipeline exercises the full stack this level built: `filter` and `map` are **stateless** operations —
each input record is processed independently, with no state store involved, which is why the `FILTER:` log
lines appear immediately and in input order, one per record, regardless of key. The `map` step changes each
record's **key** (from whatever it was — `null`, in this unkeyed input — to the extracted category), which
forces a **repartition**: Kafka Streams has to insert an internal repartition topic (the same kind seen in
Level 3, Lesson 5's example) before the subsequent `groupByKey().count()`, because downstream, same-key
records need to land on the same partition and task for the aggregation to be correct — exactly Level 1,
Lesson 4's partitioning-by-key logic, applied internally by the Streams framework itself whenever a topology
re-keys a stream.

The final `count()` is the same stateful, changelog-backed `KTable` aggregation from Lessons 4, 5, and 7 — it
would survive a crash exactly the way that lesson's demo did, for exactly the same reason: every update is
durably logged to a changelog topic before (or as) it's reflected in the local state store.

## Common mistakes

- **Forgetting that a `map` which changes the key triggers a real repartition topic.** It's easy to chain
  `filter`/`map`/`groupByKey` without noticing an extra internal topic now exists — that's a real operational
  resource (Level 4, Lesson 2's capacity math applies to it too), not a free logical transformation.
- **Expecting a `KTable` to report a zero count for keys that were always filtered out.** As this project
  shows directly with `clothing`, a key with no contributing records simply has no entry at all — treating a
  missing key as an error, rather than "no qualifying data yet," is a common misreading of `KTable` semantics.
- **Debugging a multi-stage topology by only inspecting the final output.** This project's `FILTER:` log line
  at the first stage was what made each decision individually checkable — without it, a wrong final count
  would be much harder to trace back to which specific record was dropped and why.

## Exercise

Extend this project's pipeline with one more real stage — a second filter keeping only categories with more
than one high-value order, for instance, which requires reading the `KTable`'s own output back as a stream and
filtering *that* — and verify the extended pipeline's output against the same six input records, predicting
the result before you run it.
