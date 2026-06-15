#!/bin/bash

set -e

BOOTSTRAP_SERVER=${BOOTSTRAP_SERVER:-localhost:9092}

echo "Creating Kafka topics..."

kafka-topics --create \
  --if-not-exists \
  --topic api-key-events \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server "$BOOTSTRAP_SERVER"

echo "Done."