# rivven-python

> High-performance Python bindings for the Rivven event streaming platform.

## Features

- **Native Performance**: Zero-copy message handling through Rust bindings (PyO3)
- **Async-First**: Full async/await support with Python's asyncio
- **Type-Safe**: Complete type annotations for IDE support
- **Transaction Support**: Exactly-once semantics with transactional producers
- **Authentication**: Multiple auth methods (simple, SCRAM-SHA-256)
- **Admin Operations**: Full topic and partition management

## Installation

```bash
pip install rivven
```

## Usage

### Basic Connection

```python
import asyncio
import rivven

async def main():
    # Connect to Rivven cluster
    client = await rivven.connect("localhost:9092")
    
    # Create a topic
    await client.create_topic("my-topic", partitions=3)
    
    # List topics
    topics = await client.list_topics()
    print(f"Topics: {topics}")

asyncio.run(main())
```

### Producer

```python
import asyncio
import rivven

async def produce():
    client = await rivven.connect("localhost:9092")
    producer = client.producer("my-topic")
    
    # Send a message
    offset = await producer.send(b'{"event": "login"}', key=b"user-123")
    print(f"Sent at offset: {offset}")
    
    # Send to specific partition
    await producer.send_to_partition(b"value", partition=0, key=b"key")
    
    # Batch send for better throughput
    offsets = await producer.send_batch([b"msg1", b"msg2", b"msg3"])

asyncio.run(produce())
```

### Consumer

```python
import asyncio
import rivven

async def consume():
    client = await rivven.connect("localhost:9092")
    consumer = client.consumer("my-topic", group="my-group")
    
    # Fetch messages
    messages = await consumer.fetch(max_messages=100)
    for msg in messages:
        print(f"Offset {msg.offset}: {msg.value_str()}")
    
    # Commit offsets
    await consumer.commit()
    
    # Or use async iterator (poll-retries on empty fetch, never stops)
    # With auto_commit=True (default), offsets are committed per batch.
    # Explicit commit is only needed with auto_commit=False:
    async for msg in consumer:
        print(f"Received: {msg.value_str()}")
        await consumer.commit()

asyncio.run(consume())
```

### Admin Operations

```python
import asyncio
import rivven

async def admin():
    client = await rivven.connect("localhost:9092")
    
    # Create topic
    await client.create_topic("new-topic", partitions=3, replication_factor=1)
    
    # List topics
    topics = await client.list_topics()
    print(f"Topics: {topics}")
    
    # Get topic configuration
    configs = await client.describe_topic_configs("new-topic")
    print(f"Configs: {configs}")
    
    # Modify topic configuration
    await client.alter_topic_config("new-topic", [("retention.ms", "86400000")])
    
    # Add partitions
    await client.create_partitions("new-topic", new_total=6)
    
    # Get offset for timestamp
    offset = await client.get_offset_for_timestamp("new-topic", 0, 1699900000000)
    
    # Delete records before offset
    await client.delete_records("new-topic", 0, before_offset=100)
    
    # Delete topic
    await client.delete_topic("old-topic")

asyncio.run(admin())
```

### Authentication

```python
import asyncio
import rivven

async def authenticated():
    client = await rivven.connect("localhost:9092")
    
    # Simple authentication
    await client.authenticate("username", "password")
    
    # Or SCRAM-SHA-256 authentication
    await client.authenticate_scram("username", "password")
    
    # Use client as normal
    topics = await client.list_topics()

asyncio.run(authenticated())
```

### TLS Connection

```python
import asyncio
import rivven

async def secure():
    # Connect with TLS
    client = await rivven.connect_tls(
        "localhost:9093",
        ca_cert_path="/path/to/ca.pem",
        server_name="broker.example.com",
        client_cert_path="/path/to/client.pem",  # Optional: mTLS
        client_key_path="/path/to/client.key",   # Optional: mTLS
    )
    
    topics = await client.list_topics()

asyncio.run(secure())
```

### Transactions (Exactly-Once Semantics)

```python
import asyncio
import rivven

async def transactional():
    client = await rivven.connect("localhost:9092")
    
    # Initialize transactional producer - returns a ProducerState object
    producer_state = await client.init_producer_id()
    print(f"Producer ID: {producer_state.producer_id}, Epoch: {producer_state.producer_epoch}")
    
    try:
        # Begin transaction (thread-safe: per-transactional-id lock prevents races;
        # lock entries are cleaned up after commit/abort to prevent unbounded growth)
        await client.begin_transaction("my-txn-id", producer_state)
        
        # Add partitions to transaction
        await client.add_partitions_to_txn("my-txn-id", producer_state, [
            ("my-topic", 0),
            ("my-topic", 1),
        ])
        
        # Publish with idempotent semantics - sequence is auto-incremented
        await client.publish_idempotent(
            topic="my-topic",
            value=b"message-1",
            producer_state=producer_state,
            key=b"key-1"
        )
        
        await client.publish_idempotent(
            topic="my-topic",
            value=b"message-2",
            producer_state=producer_state,
            key=b"key-2"
        )
        
        # Commit transaction
        await client.commit_transaction("my-txn-id", producer_state)
        
    except Exception as e:
        # Abort on error
        await client.abort_transaction("my-txn-id", producer_state)
        raise

asyncio.run(transactional())
```

## Exception Handling

Rivven provides a hierarchy of exception types for granular error handling:

```python
from rivven import (
    RivvenException,        # Base exception for all Rivven errors
    ConnectionException,    # Connection-related errors
    ServerException,        # Server-side errors
    TimeoutException,       # Request timeouts
    SerializationException, # Serialization/deserialization errors
    ConfigException,        # Configuration errors
)

try:
    client = await rivven.connect("localhost:9092")
except ConnectionException as e:
    print(f"Failed to connect: {e}")
except TimeoutException as e:
    print(f"Connection timed out: {e}")
except RivvenException as e:
    print(f"General Rivven error: {e}")
```

## Testing

### Running Tests

```bash
# Install test dependencies
pip install -r requirements-test.txt

# Run API tests (no broker required)
pytest tests/test_api.py -v

# Run integration tests (requires running broker)
export RIVVEN_BROKER="localhost:9092"
pytest tests/test_integration.py -v -m integration
```

### Type Checking

The package includes type stubs (`rivven.pyi`) for IDE support and static type checking:

```bash
pip install mypy
mypy your_code.py
```

## Building from Source

```bash
# Install maturin
pip install maturin

# Build wheel
cd crates/rivven-python
maturin build --release

# Install locally
pip install target/wheels/rivven-*.whl

# Development install (editable)
maturin develop --release
```

## Documentation

- [Getting Started](https://rivven.hupe1980.github.io/rivven/docs/getting-started)
- [Python Examples](https://rivven.hupe1980.github.io/rivven/docs/getting-started#python-client)

## License

Apache-2.0. See [LICENSE](../../LICENSE).
