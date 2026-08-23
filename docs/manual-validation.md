# Manual validation walkthrough (newbie-friendly)

This drives the real CLI, by hand, against a throwaway Kafka broker on your own machine. It works on
Apple Silicon (it never runs the JVM Kafka shell tools, which crash under Docker emulation on arm64).

You'll set up a consumer group stuck on a poison pill, then diagnose it, inspect the bad message,
generate a safe recovery script, and run it — watching each step with your own eyes.

## 0. Prerequisites

- **JDK 17+** and **Maven 3.9+** installed (`java -version`, `mvn -version`).
- **Docker Desktop running** (`docker info` should succeed).

## 1. Build the tool

```bash
mvn -q -pl antidote-cli -am package -DskipTests
```

This produces the runnable "fat jar" at `antidote-cli/target/antidote.jar` (it bundles Kafka's
client library, so we can reuse it later to seed data and to run the generated script).

## 2. Start a disposable broker

```bash
./scripts/local-broker.sh start
```

Wait ~5 seconds. This runs a single-node Kafka on `localhost:9092` in Docker. It is throwaway — you
delete it in step 8. (Ground rule: never point this tool at a production cluster.)

## 3. Seed a poison-pill scenario

```bash
java -cp antidote-cli/target/antidote.jar scripts/Seed.java
```

This creates topic `orders`, writes 5 messages to partition 0 with a **poison pill at offset 2**
(`{"amount":true,"quantity":"seven"}` — wrong types where numbers are expected), and parks consumer
group `orders-consumer` at committed offset 2. Result: the group is stuck with **lag 3**.

## 4. Diagnose — find the stuck offset

```bash
java -jar antidote-cli/target/antidote.jar diagnose --bootstrap localhost:9092 --group orders-consumer --poll-interval 200ms --samples 2
```

Expect it to report `orders partition 0`, `committed offset: 2`, `log-end offset: 5`, `lag: 3`, and
**exit code 1** (1 = "poison found", not an error). It samples twice 200ms apart to be sure the
offset is truly frozen, not just slow.

## 5. Inspect — see the bad message

```bash
java -jar antidote-cli/target/antidote.jar inspect --bootstrap localhost:9092 --topic orders --partition 0 --offset 2
```

Expect a full dump: a heuristic classification, metadata (key `order-2`, timestamp, size), a hex view,
and the readable `Best-effort UTF-8`: `{"id":"order-2","amount":true,"quantity":"seven"}`. Exit 0.

## 6. Generate the safe recovery script

Create a corrected version of the message, then generate the re-injection script:

```bash
printf '{"id":"order-2","amount":42,"quantity":7}' > /tmp/fixed-order.json
```

```bash
java -jar antidote-cli/target/antidote.jar gen-reinject --bootstrap localhost:9092 --topic orders --partition 0 --offset 2 --corrected-payload /tmp/fixed-order.json --out /tmp/Reinject.java
```

**Open `/tmp/Reinject.java` and read it.** It has a WARNING header, targets the exact partition
(`PARTITION = 0`), and defaults to a dry run. This review step is also the project's required
human-review gate for the safety-critical code.

## 7. Run the script — dry run first, then for real

```bash
java -cp antidote-cli/target/antidote.jar /tmp/Reinject.java
```

Dry run: prints what it *would* send and produces **nothing**.

```bash
java -cp antidote-cli/target/antidote.jar /tmp/Reinject.java --produce --yes
```

Now it actually sends, printing `Produced corrected message to orders-0@5`. Confirm the corrected
bytes are there, and that the group is **still stuck** (re-injection preserves data, it does not
unstick — you'd fix the root cause / advance the group as a separate deliberate step):

```bash
java -jar antidote-cli/target/antidote.jar inspect --bootstrap localhost:9092 --topic orders --partition 0 --offset 5
```

```bash
java -jar antidote-cli/target/antidote.jar diagnose --bootstrap localhost:9092 --group orders-consumer --poll-interval 200ms --samples 2
```

The inspect shows `{"id":"order-2","amount":42,"quantity":7}`; the diagnose still shows
`committed offset: 2` (lag is now 4 because there's one more message on the topic). That's correct.

## 8. Tear down

```bash
./scripts/local-broker.sh stop
```

## Try `--json`

Every command takes `--json` for scripting, e.g.:

```bash
java -jar antidote-cli/target/antidote.jar diagnose --bootstrap localhost:9092 --group orders-consumer --json
```
