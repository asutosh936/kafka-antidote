# The poison pill in your Kafka pipeline — and a two-minute antidote

> **Status: DRAFT launch artifact — not for publication yet.** This is the deep-dive write-up drafted
> as part of the v0.1 ship gate. Review and edit before it goes anywhere public.

## The 2am problem

A single malformed message lands in a Kafka topic. Your consumer tries to deserialize it, throws,
retries, throws again — and stops. It never commits past that offset. Every healthy message behind
the bad one now waits. Lag climbs. A pager goes off.

This is the **poison pill**, and if you run Kafka long enough you will meet it. The frustrating part
isn't the concept — it's the recovery. At 2am, half-awake, you're doing log archaeology: grepping for
the exception, guessing the partition, trying to eyeball an un-deserializable payload, and then very
carefully deciding how to get the consumer moving again *without* losing data or skipping good
messages. It's fiddly, it's stressful, and it's exactly the kind of task where a tired human makes
the mistake that turns an incident into an outage.

**Kafka Antidote** replaces that archaeology with one command.

## What it does

Three read-first steps, one careful write:

- `diagnose` — connects to the consumer group, samples offsets twice, and tells you exactly which
  partition is frozen: topic, partition, committed offset, log-end offset, lag. Read-only.
- `inspect` — fetches the raw bytes at the stuck offset **without** routing them through the
  deserializer that's failing, and shows them three ways (hex, best-effort UTF-8, and a heuristic
  guess at *why* it failed). Read-only.
- `gen-reinject` — generates a small, reviewable script that re-injects a corrected message to the
  **exact** original partition. It does not run it. You review it, then you run it.

The primary user story is a two-minute path from "the group is stalled" to "I understand it and I
have a safe script in hand" — without reading a single raw log line.

## Three design decisions worth explaining

### 1. It never unsticks the group for you — on purpose

The most tempting feature is also the most dangerous: "just skip the bad offset automatically." We
deliberately don't. Re-injection **preserves the good data** (the corrected message) but leaves the
consumer parked exactly where it was. Advancing past a poison pill mutates offsets, and auto-mutating
offsets is how a recovery tool turns a one-message incident into silent data loss. Unsticking is a
separate, deliberate, human-initiated action. The generated script literally cannot do it: it
contains no consumer, no `seek`, no `commit`.

### 2. Detection distinguishes "stuck" from "slow"

A single snapshot of "lag > 0" can't tell a poison pill from a consumer that's merely behind. So
detection samples the committed offset at least twice over an interval: a partition is only reported
stuck if its committed offset **doesn't move** while lag stays non-zero. Fewer false pages.

### 3. The generated script targets the exact partition — by construction

Re-injecting "to the same topic" isn't enough; a normal producer would re-partition by key hash and
could land the message somewhere else. The generated script uses the explicit-partition
`ProducerRecord` constructor and asserts at runtime that it landed where intended. It defaults to a
dry run and requires `--produce --yes` to actually send. Its header tells you, in bold, to fix the
root cause first.

## How it's built to stay honest

- **One boundary.** Everything — the CLI, the classifier, the re-injection generator — depends only
  on a single `MessageSource` interface and a handful of immutable value types. Exactly one class
  touches the Kafka client library. That's what makes the roadmap (Kafka Streams next, then Connect)
  additive: a new source plugs in without disturbing anything else.
- **Integration tests are the truth.** Every core guarantee is proven against a real broker spun up
  in Docker — including a test that *compiles and runs the generated re-injection script* and checks
  the corrected message lands on the exact partition. Thousands of random malformed payloads are
  thrown at the classifier to prove it never crashes.
- **Correctness over features.** v0.1 supports plain consumers and nothing more. No speculative
  abstractions, no half-built Streams support. A smaller tool that never corrupts state beats a
  larger one that might.

## Try it

```bash
mvn -q -pl antidote-cli -am package -DskipTests
./scripts/local-broker.sh start
java -cp antidote-cli/target/antidote.jar scripts/Seed.java
java -jar antidote-cli/target/antidote.jar diagnose --bootstrap localhost:9092 --group orders-consumer
```

Full walkthrough: [docs/manual-validation.md](manual-validation.md). Contributions welcome — the
natural first one is the v0.2 Kafka Streams `MessageSource`.

---

*Draft. TODO before launch: add a screen recording / asciinema of the golden path; confirm the
license; decide where this publishes; add real-world war-story framing from an actual incident.*
