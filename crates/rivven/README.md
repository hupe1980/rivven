# rivven

Command-line interface for Rivven event streaming platform.

## Installation

```bash
cargo install rivven
```

## Commands

### Topic Management

```bash
# List topics
rivven topic list --server localhost:9092

# Create topic
rivven topic create my-topic --partitions 3 --server localhost:9092

# Delete topic
rivven topic delete my-topic --server localhost:9092

# Describe topic
rivven topic describe my-topic --server localhost:9092
```

### Produce Messages

```bash
# Produce from stdin
echo '{"event": "test"}' | rivven produce my-topic --server localhost:9092

# Produce with key
rivven produce my-topic --key user-123 --value '{"action": "login"}' --server localhost:9092

# Produce from file
rivven produce my-topic --file events.jsonl --server localhost:9092
```

### Consume Messages

```bash
# Consume from beginning
rivven consume my-topic --from-beginning --server localhost:9092

# Consume with group
rivven consume my-topic --group my-group --server localhost:9092

# Consume specific partition
rivven consume my-topic --partition 0 --offset 100 --server localhost:9092

# Consume with limit
rivven consume my-topic --max-messages 10 --server localhost:9092
```

### Cluster Operations

```bash
# Cluster status
rivven cluster status --server localhost:9092

# List brokers
rivven cluster brokers --server localhost:9092

# Consumer groups
rivven group list --server localhost:9092
rivven group describe my-group --server localhost:9092
```

## Configuration

Environment variables:
- `RIVVEN_BOOTSTRAP_SERVERS` - Default bootstrap servers
- `RIVVEN_TLS_ENABLED` - Enable TLS
- `RIVVEN_TLS_CA_FILE` - CA certificate file

## Output Formats

```bash
# JSON output
rivven topic list --output json

# Table output (default)
rivven topic list --output table

# Compact output
rivven topic list --output compact
```

## Related Crates

- [`rivvend`](https://crates.io/crates/rivvend) - Broker server daemon
- [`rivven-client`](https://crates.io/crates/rivven-client) - Rust client library
- [`rivven-connect`](https://crates.io/crates/rivven-connect) - Connector framework

## License

See root [LICENSE](../../LICENSE) file.
