---
description: "Real Avro schema evolution: a backward-compatible read filling a default value, and an incompatible schema failing with a real exception naming the field."
---

# 08 · Schema Evolution: Avro/Protobuf & Compatibility

A producer and a consumer almost never run the exact same code version at the exact same moment in a real
system — which means the schema a producer writes with and the schema a consumer reads with will eventually
diverge. This lesson runs real Avro serialization to show exactly what "compatible" and "incompatible" schema
changes actually do, bytes and all.

!!! note "What was run, and what wasn't"
    Confluent **Schema Registry** — the component that stores schemas centrally and enforces compatibility
    rules automatically across producers and consumers — is a separate project, not bundled with Apache Kafka,
    and wasn't installed for this lab. What **was** run for real: the Apache Avro library itself (fetched
    directly from Maven Central), performing genuine binary serialization and schema-evolved deserialization,
    which is the actual mechanism Schema Registry's compatibility checking is built on top of. The registry
    adds centralized storage and automatic enforcement; the compatibility rules themselves are exactly what
    this lesson demonstrates directly against Avro's own resolution logic.

## The core idea: writer schema and reader schema can differ

Avro records are serialized **without** their schema embedded in the bytes (unlike, say, JSON, which carries
its own field names inline) — the schema has to be known separately by whoever reads the bytes back. Avro's
resolution logic can reconcile a **writer schema** (what the producer used) against a different **reader
schema** (what the consumer expects), following specific rules about what's allowed to differ.

## Worked example: a real compatible read, and a real incompatible one

```java
// Schema V1: name, age
GenericRecord recordV1 = ...;  // name="alice", age=30
byte[] bytesWrittenWithV1 = ...;  // serialized with schema V1

// Schema V2: adds "email" WITH a default value
GenericDatumReader<GenericRecord> readerV2 = new GenericDatumReader<>(schemaV1, schemaV2);
GenericRecord recordReadAsV2 = readerV2.read(null, decoder);

// Schema V3: adds "requiredNewField" WITHOUT a default value
GenericDatumReader<GenericRecord> readerV3 = new GenericDatumReader<>(schemaV1, schemaV3);
```

```text
Wrote 7 bytes with schema V1 (name, age)
Read with schema V2 (compatible): {"name": "alice", "age": 30, "email": "unknown@example.com"}
Reading with schema V3 (new required field, no default) FAILED as expected:
  org.apache.avro.AvroTypeException: Found User, expecting User, missing required field requiredNewField
```

(Real output — genuine Avro binary encoding and decoding, no registry, no mocking.) The original record
serialized to just **7 bytes** — Avro's binary format is compact precisely because it doesn't repeat field
names or type tags the schema already specifies elsewhere. Reading those 7 bytes with **schema V2** (which
adds an `email` field *with a default value*) succeeded perfectly: the reader filled in the default
(`"unknown@example.com"`) for the field that simply wasn't present in the original bytes. Reading the same 7
bytes with **schema V3** (which adds `requiredNewField` *without* a default) failed with a real, specific
exception naming exactly the missing field — Avro has no way to invent a value for a required field that was
never written and has no fallback.

## How It Actually Works

This is the entire practical rule behind Avro's (and Protobuf's, which follows very similar reasoning) schema
evolution: **adding a field is safe only if you also give it a default value** — readers using the newer
schema against data written with the older one have somewhere to fall back to. Removing a field, conversely,
is safe for *old readers reading new data* only if the removed field itself had a default (so old readers
expecting it still get something sensible) — the compatibility rules aren't arbitrary, they follow directly
from "can the reader always produce a complete, valid record from what's actually present in the bytes."
**Backward compatibility** means new schema versions can read data written by old ones (what this lesson's V2
read demonstrated); **forward compatibility** means old schema versions can read data written by new ones —
real systems often need both simultaneously, because producers and consumers upgrade independently and there's
no way to guarantee which side updates first.

This is precisely the problem **Schema Registry** (and equivalents) solve operationally, on top of these same
underlying rules: rather than every team manually tracking which schema versions are compatible with which,
the registry stores every schema version centrally, checks a *new* schema against compatibility rules before
allowing it to be registered at all, and serves schemas to consumers by ID so they don't need the schema
bundled with every message the way this lesson's raw demo does.

## Common mistakes

- **Adding a required field without a default "temporarily," planning to backfill it later.** As shown
  directly, this immediately breaks any reader still using the old schema against new data, or any attempt to
  read old data with the new schema — there's no grace period at the serialization level.
- **Renaming a field and assuming it's a safe, compatible change.** To Avro's resolution logic, a rename is
  indistinguishable from "remove the old field, add an unrelated new one" — both changes need to be safe on
  their own (the removed field needs a default; the added field needs a default) for the rename to actually be
  compatible.
- **Treating "it worked once" as proof of compatibility.** This lesson tested exactly one specific writer/
  reader schema pair — a real compatibility *checker* (Schema Registry's own validation, or Avro's
  `SchemaCompatibility` API) is what should gate a new schema version before it ships, not one manual test.

## Exercise

Reproduce this lesson's exact test: serialize a record with one Avro schema, then deserialize it with a second
schema that adds a field with a default (confirm it works) and a third schema that adds a required field with
no default (confirm it fails, and read the exact exception message). Then try removing a field entirely
(instead of adding one) and reason about which direction — old schema reading new data, or new schema reading
old data — is actually affected.
