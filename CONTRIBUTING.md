# Contributing to Kafka Antidote

Thanks for your interest! This project values **correctness over features** — a smaller tool that
never corrupts state beats a larger one that might. Please read this before opening a PR.

## Ground rules

1. **Disposable brokers only.** All development and testing use a local/throwaway broker
   (Testcontainers). Never point tests or development at a production cluster.
2. **Tests come first.** For any behavior change: write a test that encodes it and watch it fail,
   then write the minimum code to pass, then refactor while green.
3. **Respect the boundary (§4 of the spec).** The CLI, payload inspector, classifier, and
   re-injection generator depend **only** on the `MessageSource` interface and the immutable value
   types in `com.kafkaantidote.core` — never on Kafka client classes directly. A concrete source is
   chosen in exactly one place: `MessageSources`.
4. **Safety-critical code needs human review.** Any change to the re-injection generator
   (`com.kafkaantidote.reinject`) or to code that could mutate cluster/offset state must be read by a
   maintainer personally — passing tests is necessary, not sufficient.

## Building & testing

```bash
mvn test      # fast unit tests, no Docker
mvn verify    # adds the real-broker integration tests (Docker must be running)
```

The behavior contract lives in the spec; the build order lives in the implementation plan. Both are
the source of truth — update the spec (and its status tracker) when behavior changes, in the same PR.

## Project layout

- `poison-fixtures/` — the standalone poison-pill corpus generator (no Kafka dependency).
- `antidote-cli/`
  - `core/` — the `MessageSource` boundary + immutable value types.
  - `consumer/` — `ConsumerMessageSource`, the only code that touches `kafka-clients`.
  - `payload/` — the heuristic classifier and the payload presenter.
  - `reinject/` — the re-injection script generator (safety-critical).
  - `cli/` — picocli commands and the `MessageSources` wiring point.

## Adding a new message source (the natural first contribution)

v0.1 ships one implementation, `ConsumerMessageSource`. Kafka Streams (v0.2) and Connect (v0.3) are
planned as **new** `MessageSource` implementations. Adding one should touch only its own package plus
a single wiring line in `MessageSources` — if it requires changes elsewhere, the boundary has been
violated. See [docs/good-first-issues.md](docs/good-first-issues.md).

## Style

- Java 17, no heavyweight framework.
- Keep the dependency surface small (`kafka-clients`, `picocli`, plus test-only `Testcontainers` and
  `jqwik`). Question anything else.
- Match the surrounding code's naming, comment density, and idioms.
