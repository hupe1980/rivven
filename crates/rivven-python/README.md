# rivven-python

Python bindings for Rivven using PyO3.

## Installation

```bash
pip install rivven
```

## Usage

### Producer

```python
from rivven import Client, ProducerConfig

client = Client.connect("localhost:9092")
producer = client.producer()

# Send a message
producer.send("my-topic", key=b"user-123", value=b'{"event": "login"}')

# Send with headers
producer.send(
    "my-topic",
    key=b"user-123",
    value=b'{"event": "purchase"}',
    headers={"source": "api", "version": "1.0"}
)

# Flush and close
producer.flush()
```

### Consumer

```python
from rivven import Client, ConsumerConfig

client = Client.connect("localhost:9092")
consumer = client.consumer(
    group_id="my-group",
    topics=["my-topic"]
)

# Poll for messages
for record in consumer.poll(timeout_ms=1000):
    print(f"Key: {record.key}, Value: {record.value}")
    consumer.commit(record)

# Or use iterator
for record in consumer:
    process(record)
    consumer.commit(record)
```

### Admin Operations

```python
from rivven import Client

client = Client.connect("localhost:9092")

# Create topic
client.create_topic("new-topic", partitions=3, replication_factor=2)

# List topics
topics = client.list_topics()
for topic in topics:
    print(f"{topic.name}: {topic.partitions} partitions")

# Delete topic
client.delete_topic("old-topic")
```

### Async Support

```python
import asyncio
from rivven import AsyncClient

async def main():
    client = await AsyncClient.connect("localhost:9092")
    producer = await client.producer()
    
    await producer.send("my-topic", key=b"key", value=b"value")
    await producer.flush()

asyncio.run(main())
```

## Configuration

```python
from rivven import ClientConfig

config = ClientConfig(
    bootstrap_servers=["node1:9092", "node2:9092"],
    connection_timeout_ms=10000,
    request_timeout_ms=30000,
)

client = Client.connect_with_config(config)
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
```

## License

See root [LICENSE](../../LICENSE) file.
