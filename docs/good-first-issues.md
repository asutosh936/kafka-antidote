# Good first issues

Candidate starter tasks for new contributors. Each is scoped to respect the architecture boundary
(§4 of the spec). When this project is on GitHub, file these as issues tagged `good first issue`.

### 1. `StreamsMessageSource` — Kafka Streams support (v0.2) — the flagship
Add a second `MessageSource` implementation for Kafka Streams' error-handling hooks, in its own
package `com.kafkaantidote.streams`, wired in via one line in `MessageSources`. This is the primary
proof that the boundary works: it should touch **zero** files outside its own package plus that one
wiring line. Spec-drive it: define the behavior, write failing integration tests, implement, harden.

### 2. `--timeout` flag for `diagnose` / `inspect`
Expose the fetch/admin client timeouts (currently fixed at 8s/15s in `ConsumerMessageSource`) as a
CLI option, parsed with the existing `DurationParser`. Add tests for parsing and for a bounded
failure against an unreachable broker.

### 3. Colorized human output (auto-off when not a TTY)
Add optional ANSI coloring to the `diagnose`/`inspect` output (e.g. red for lag, yellow for the
heuristic label), disabled when stdout is not a terminal or when `--no-color` is passed. Pure
presentation change in `payload.PayloadPresenter` and the commands.

### 4. SASL/SSL connection options
Allow secured clusters by passing through `--command-config <file>` (a standard Kafka client
properties file) to the Admin/consumer builders. Keep read and write credentials separable (R6.3).

### 5. A `Brewfile` / install script for the golden-path quick start
Make `java -jar antidote.jar` easier to obtain — a small install script or Homebrew formula that
fetches the release jar, so the README quick start works with one command.
