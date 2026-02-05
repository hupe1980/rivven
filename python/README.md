# Rivven Python SDK

High-performance Python bindings for the [Rivven](https://github.com/hupe1980/rivven) distributed streaming platform, built with [PyO3](https://pyo3.rs/).

## Features

- **Native Performance**: Zero-copy message handling through Rust bindings
- **Async-First**: Full async/await support with Python's asyncio
- **Type-Safe**: Complete type annotations for IDE support
- **Easy to Use**: Pythonic API design with familiar patterns
- **Transaction Support**: Exactly-once semantics with transactional producers
- **Authentication**: Multiple auth methods (simple, SCRAM-SHA-256)
- **Admin Operations**: Full topic and partition management

## Installation

### From PyPI (when published)

```bash
pip install rivven
```

### From Source

Requires [Rust](https://rustup.rs/) and [maturin](https://github.com/PyO3/maturin):

```bash
# Install maturin
pip install maturin

# Build and install
cd crates/rivven-python
maturin develop
```

For release builds:

```bash
maturin develop --release
```

## Quick Start

### Connecting to Rivven

```python
import asyncio
import rivven

async def main():
    # Connect to a Rivven cluster
    client = await rivven.connect("localhost:9092")
    
    # Create a topic
    await client.create_topic("my-topic", partitions=3)
    
    # List topics
    topics = await client.list_topics()
    print(f"Topics: {topics}")

asyncio.run(main())
```

### Producing Messages

```python
import asyncio
import rivven

async def produce():
    client = await rivven.connect("localhost:9092")
    producer = client.producer("my-topic")
    
    # Send a single message
    offset = await producer.send(b"Hello, Rivven!")
    print(f"Message sent at offset: {offset}")
    
    # Send with a key (for partitioning)
    await producer.send(b"value", key=b"user-123")
    
    # Send to a specific partition
    await producer.send_to_partition(b"value", partition=0)
    
    # Batch send for better throughput
    messages = [b"msg1", b"msg2", b"msg3"]
    offsets = await producer.send_batch(messages)

asyncio.run(produce())
```

### Consuming Messages

```python
import asyncio
import rivven

async def consume():
    client = await rivven.connect("localhost:9092")
    consumer = client.consumer("my-topic", group_id="my-group")
    
    # Fetch a batch of messages
    messages = await consumer.fetch(max_messages=100)
    for msg in messages:
        print(f"Offset {msg.offset}: {msg.value.decode()}")
        if msg.key:
            print(f"  Key: {msg.key.decode()}")
    
    # Commit offsets
    await consumer.commit()

asyncio.run(consume())
```

### Async Iterator Pattern

```python
import asyncio
import rivven

async def stream():
    client = await rivven.connect("localhost:9092")
    consumer = client.consumer("my-topic", group_id="my-group")
    
    # Process messages as an async stream
    async for message in consumer:
        print(f"Received: {message.value_str()}")
        
        # Process and commit
        await consumer.commit()

asyncio.run(stream())
```

### TLS Connection

```python
import asyncio
import rivven

async def secure_connect():
    # Connect with TLS
    client = await rivven.connect_tls(
        "localhost:9093",
        ca_cert="/path/to/ca.crt",
        client_cert="/path/to/client.crt",  # Optional
        client_key="/path/to/client.key",   # Optional
    )
    
    # Use client as normal
    topics = await client.list_topics()

asyncio.run(secure_connect())
```

### Authentication

```python
import asyncio
import rivven

async def authenticated_connect():
    client = await rivven.connect("localhost:9092")
    
    # Simple username/password authentication
    await client.authenticate("username", "password")
    
    # Or use SCRAM-SHA-256 authentication
    await client.authenticate_scram("username", "password")
    
    # Use client as normal
    topics = await client.list_topics()

asyncio.run(authenticated_connect())
```

### Transactions (Exactly-Once Semantics)

```python
import asyncio
import rivven

async def transactional_produce():
    client = await rivven.connect("localhost:9092")
    
    # Initialize transactional producer
    producer_id, epoch = await client.init_producer_id("my-txn-id")
    
    try:
        # Begin transaction
        await client.begin_transaction("my-txn-id", producer_id, epoch)
        
        # Publish with idempotent semantics
        await client.publish_idempotent(
            topic="my-topic",
            value=b"message-1",
            producer_id=producer_id,
            epoch=epoch,
            sequence=0,
            key=b"key-1"
        )
        await client.publish_idempotent(
            topic="my-topic",
            value=b"message-2",
            producer_id=producer_id,
            epoch=epoch,
            sequence=1,
            key=b"key-2"
        )
        
        # Commit transaction
        await client.commit_transaction("my-txn-id", producer_id, epoch)
        print("Transaction committed successfully")
        
    except Exception as e:
        # Abort transaction on error
        await client.abort_transaction("my-txn-id", producer_id, epoch)
        print(f"Transaction aborted: {e}")

asyncio.run(transactional_produce())
```

### Admin Operations

```python
import asyncio
import rivven

async def admin_operations():
    client = await rivven.connect("localhost:9092")
    
    # Create topic with multiple partitions
    await client.create_topic("my-topic", partitions=3, replication_factor=1)
    
    # Get topic configuration
    configs = await client.describe_topic_configs("my-topic")
    print(f"Topic configs: {configs}")
    
    # Modify topic configuration
    await client.alter_topic_config("my-topic", "retention.ms", "86400000")
    
    # Add more partitions
    await client.create_partitions("my-topic", new_total=6)
    
    # Get offset for a specific timestamp
    offset = await client.get_offset_for_timestamp("my-topic", partition=0, timestamp_ms=1699900000000)
    print(f"Offset at timestamp: {offset}")
    
    # Delete records before a specific offset
    deleted = await client.delete_records("my-topic", partition=0, before_offset=100)
    print(f"Deleted records, new low watermark: {deleted}")

asyncio.run(admin_operations())
```

## API Reference

### Module Functions

#### `rivven.connect(addr: str, timeout_ms: int = 5000) -> RivvenClient`

Connect to a Rivven cluster.

- `addr`: Server address (e.g., "localhost:9092")
- `timeout_ms`: Connection timeout in milliseconds
- Returns: `RivvenClient` instance

#### `rivven.connect_tls(addr, ca_cert, client_cert=None, client_key=None, timeout_ms=5000)`

Connect with TLS encryption.

#### `rivven.version() -> str`

Returns the SDK version string.

### RivvenClient

Main client class for interacting with Rivven.

#### Topic Management

- `create_topic(name, partitions=1, replication_factor=1)` - Create a new topic
- `delete_topic(name)` - Delete a topic
- `list_topics()` - List all topics
- `get_metadata(topic)` - Get detailed topic metadata
- `describe_topic_configs(topic)` - Get topic configuration
- `alter_topic_config(topic, key, value)` - Modify topic configuration
- `create_partitions(topic, new_total)` - Add partitions to a topic
- `delete_records(topic, partition, before_offset)` - Delete records before offset
- `get_offset_for_timestamp(topic, partition, timestamp_ms)` - Get offset for timestamp

#### Producer/Consumer

- `producer(topic)` - Get a producer for the topic
- `consumer(topic, group_id=None, auto_commit=True, isolation_level=None)` - Get a consumer

#### Consumer Groups

- `list_groups()` - List all consumer groups
- `describe_group(group_id)` - Get group details
- `delete_group(group_id)` - Delete a consumer group
- `commit_offset(group_id, topic, partition, offset)` - Commit offset
- `get_offset(group_id, topic, partition)` - Get committed offset

#### Authentication

- `authenticate(username, password)` - Simple authentication
- `authenticate_scram(username, password)` - SCRAM-SHA-256 authentication

#### Transactions

- `init_producer_id(transactional_id)` - Initialize transactional producer, returns (producer_id, epoch)
- `begin_transaction(transactional_id, producer_id, epoch)` - Begin a transaction
- `commit_transaction(transactional_id, producer_id, epoch)` - Commit a transaction
- `abort_transaction(transactional_id, producer_id, epoch)` - Abort a transaction
- `publish_idempotent(topic, value, producer_id, epoch, sequence, key=None)` - Publish with exactly-once semantics

#### Health

- `ping()` - Check server connectivity

### Producer

Producer for publishing messages to a topic.

- `send(value, key=None)` - Send a single message
- `send_to_partition(value, partition, key=None)` - Send to specific partition
- `send_batch(values, keys=None)` - Send multiple messages

### Consumer

Consumer for reading messages from a topic.

- `fetch(max_messages=100, timeout_ms=5000)` - Fetch messages
- `commit()` - Commit current offsets
- `seek(partition, offset)` - Seek to specific offset
- `seek_to_beginning(partition)` - Seek to start
- `seek_to_end(partition)` - Seek to end
- `get_offset_bounds(partition)` - Get min/max offsets
- Async iteration: `async for msg in consumer`

### Message

A consumed message.

- `value` (bytes): Message payload
- `key` (bytes | None): Optional message key
- `offset` (int): Partition offset
- `timestamp` (int): Unix timestamp (ms)
- `partition` (int): Partition ID
- `topic` (str): Topic name
- `value_str()` - Get value as UTF-8 string
- `key_str()` - Get key as UTF-8 string

### RivvenError

Exception raised for Rivven errors.

- `message` (str): Error description
- `kind` (str): Error category (Connection, Server, Timeout, etc.)

## Development

### Building

```bash
# Install development dependencies
pip install maturin pytest pytest-asyncio

# Build in development mode
maturin develop

# Run tests
pytest tests/
```

### Running Tests

```bash
# Unit tests
pytest tests/unit/

# Integration tests (requires running Rivven server)
pytest tests/integration/
```

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](../LICENSE) for details.
