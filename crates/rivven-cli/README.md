# rivven-cli

Command-line interface for Rivven.

## Installation

```bash
cargo install rivven-cli
# or build from source
cargo build -p rivven-cli --release
```

## Commands

### Topic Management

```bash
# List topics
rivvenctl topic list --server localhost:9092

# Create topic
rivvenctl topic create my-topic --partitions 3 --server localhost:9092

# Delete topic
rivvenctl topic delete my-topic --server localhost:9092

# Describe topic
rivvenctl topic describe my-topic --server localhost:9092
```

### Produce Messages

```bash
# Produce from stdin
echo '{"event": "test"}' | rivvenctl produce my-topic --server localhost:9092

# Produce with key
rivvenctl produce my-topic --key user-123 --value '{"action": "login"}' --server localhost:9092

# Produce from file
rivvenctl produce my-topic --file events.jsonl --server localhost:9092
```

### Consume Messages

```bash
# Consume from beginning
rivvenctl consume my-topic --from-beginning --server localhost:9092

# Consume with group
rivvenctl consume my-topic --group my-group --server localhost:9092

# Consume specific partition
rivvenctl consume my-topic --partition 0 --offset 100 --server localhost:9092

# Consume with limit
rivvenctl consume my-topic --max-messages 10 --server localhost:9092
```

### Cluster Operations

```bash
# Cluster status
rivvenctl cluster status --server localhost:9092

# List brokers
rivvenctl cluster brokers --server localhost:9092

# Consumer groups
rivvenctl group list --server localhost:9092
rivvenctl group describe my-group --server localhost:9092
```

## Configuration

Environment variables:
- `RIVVEN_BOOTSTRAP_SERVERS` - Default bootstrap servers
- `RIVVEN_TLS_ENABLED` - Enable TLS
- `RIVVEN_TLS_CA_FILE` - CA certificate file

## Output Formats

```bash
# JSON output
rivvenctl topic list --output json

# Table output (default)
rivvenctl topic list --output table

# Compact output
rivvenctl topic list --output compact
```

## License

See root [LICENSE](../../LICENSE) file.
