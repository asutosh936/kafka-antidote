#!/usr/bin/env bash
# Start/stop a disposable single-node Kafka broker on localhost:9092 for MANUAL testing.
# Uses the GraalVM-native image so it runs on Apple Silicon without the JVM-broker SIGILL crash.
#
#   ./scripts/local-broker.sh start   # boot it (wait ~5s before using)
#   ./scripts/local-broker.sh stop    # tear it down
set -euo pipefail
NAME=antidote-kafka

case "${1:-start}" in
  start)
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    docker run -d --name "$NAME" -p 9092:9092 \
      -e KAFKA_NODE_ID=1 \
      -e KAFKA_PROCESS_ROLES=broker,controller \
      -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
      -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
      -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
      -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
      -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
      -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
      -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
      apache/kafka-native:3.8.1 >/dev/null
    echo "Started '$NAME' on localhost:9092. Give it ~5 seconds to become ready."
    ;;
  stop)
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    echo "Stopped and removed '$NAME'."
    ;;
  *)
    echo "usage: $0 {start|stop}" >&2
    exit 2
    ;;
esac
