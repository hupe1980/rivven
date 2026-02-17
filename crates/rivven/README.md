# rivven

> Command-line interface for the Rivven event streaming platform.

## Overview

`rivven` provides a CLI for managing topics, producing/consuming messages, and administering consumer groups. Designed for both interactive use and scripting.

For CDC (Change Data Capture), use the separate `rivven-connect` binary.

## Installation

```bash
cargo install rivven
```

## Quick Start

```bash
# Check connectivity
rivven ping --server 127.0.0.1:9092

# Create a topic
rivven topic create my-topic --partitions 3

# Produce a message
rivven produce my-topic "Hello, Rivven!"

# Consume messages
rivven consume my-topic --partition 0 --offset 0 --max 10
```

## Commands

### Topic Management

```bash
# List all topics
rivven topic list

# Create topic with partitions (default: 3)
rivven topic create my-topic --partitions 6

# Get topic metadata (name + partition count)
rivven topic info my-topic

# Delete a topic
rivven topic delete my-topic
```

### Produce Messages

```bash
# Produce a message (positional arguments: topic, message)
rivven produce my-topic "Hello, Rivven!"

# Produce with a key
rivven produce my-topic '{"event": "login"}' --key user-123

# Produce to a specific partition
rivven produce my-topic "partitioned" --partition 0

# Connect to a different server
rivven produce my-topic "msg" --server 10.0.0.1:9092
```

### Consume Messages

```bash
# Consume from partition 0, offset 0 (defaults)
rivven consume my-topic

# Consume from a specific partition and offset
rivven consume my-topic --partition 2 --offset 100

# Limit number of messages (default: 100)
rivven consume my-topic --max 10

# Follow mode — continuously consume new messages (like tail -f)
rivven consume my-topic --follow
```

### Consumer Group Management

```bash
# List all consumer groups
rivven group list

# Describe a group (show committed offsets per topic/partition)
rivven group describe my-group

# Delete a consumer group
rivven group delete my-group
```

### Connectivity

```bash
# Ping the server
rivven ping

# Ping a specific server
rivven ping --server 10.0.0.1:9092
```

## Global Options

All commands accept `--server <address>` to override the default server address (`127.0.0.1:9092`).

## Documentation

- [Getting Started](https://rivven.hupe1980.github.io/rivven/docs/getting-started)

## License

Apache-2.0. See [LICENSE](../../LICENSE).
