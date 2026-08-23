#!/usr/bin/env bash
#
# Live demo of `antidote diagnose` (Phase 1). Boots a disposable Kafka broker via Testcontainers,
# creates a consumer group stuck at offset 2 (lag 3), and runs the real diagnose command so you can
# watch its output. Requires Docker to be running.
#
# Usage:  ./demo.sh
#
set -euo pipefail
cd "$(dirname "$0")"

echo "Building and running the live diagnose demo (needs Docker)..."
mvn -q -pl antidote-cli -am verify \
    -Dantidote.demo=true \
    -Dit.test=DiagnoseDemoIT \
    -Dfailsafe.failIfNoSpecifiedTests=false \
    -Dsurefire.skip=true \
    -Dmaven.test.redirectTestOutputToFile=true

echo
cat antidote-cli/target/failsafe-reports/com.kafkaantidote.demo.DiagnoseDemoIT-output.txt
