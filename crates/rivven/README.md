# rivven

> Command-line interface for the Rivven event streaming platform.

## Overview

`rivven` provides a unified CLI for managing topics, producing/consuming messages, and administering clusters. Designed for both interactive use and scripting.

## Installation

```bash
cargo install rivven
```

## Quick Start

```bash
# Create a topic
rivven topic create my-topic --partitions 3

# Produce messages
rivven produce my-topic "Hello, Rivven!"

# Consume messages
rivven consume my-topic --from-beginning
```

## Commands

### Topic Management

```bash
# List topics
rivven topic list

# Create topic with partitions
rivven topic create my-topic --partitions 3

# Describe topic details
rivven topic describe my-topic

# Delete topic
rivven topic delete my-topic
```

### Produce Messages

```bash
# Produce from stdin
echo '{"event": "test"}' | rivven produce my-topic

# Produce with key
rivven produce my-topic --key user-123 --value '{"action": "login"}'

# Produce from file
rivven produce my-topic --file events.jsonl
```

### Consume Messages

```bash
# Consume from beginning
rivven consume my-topic --from-beginning

# Consume with consumer group
rivven consume my-topic --group my-group

# Consume specific partition/offset
rivven consume my-topic --partition 0 --offset 100

# Consume with limit
rivven consume my-topic --max-messages 10
```

### Cluster Operations

```bash
# Cluster status
rivven cluster status

# List brokers
rivven cluster brokers

# Consumer groups
rivven group list
rivven group describe my-group
```

## Configuration

| Variable | Description |
|:---------|:------------|
| `RIVVEN_BOOTSTRAP_SERVERS` | Default bootstrap servers |
| `RIVVEN_TLS_ENABLED` | Enable TLS |
| `RIVVEN_TLS_CA_FILE` | CA certificate file |

## Output Formats

```bash
rivven topic list --output json    # JSON output
rivven topic list --output table   # Table output (default)
rivven topic list --output compact # Compact output
```

## Documentation

- [Getting Started](https://rivven.hupe1980.github.io/rivven/docs/getting-started)
- [CLI Reference](https://rivven.hupe1980.github.io/rivven/docs/cli)

## License

Apache-2.0. See [LICENSE](../../LICENSE).
