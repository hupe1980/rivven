"""Type stubs for rivven._rivven native module"""

from typing import Optional, List, Tuple, Any, AsyncIterator

class RivvenError(Exception):
    """Exception raised for Rivven operations"""
    
    @property
    def message(self) -> str:
        """Error message"""
        ...
    
    @property
    def kind(self) -> str:
        """Error category (Connection, Server, Timeout, etc.)"""
        ...

class Message:
    """A message consumed from a Rivven topic"""
    
    @property
    def value(self) -> bytes:
        """Message payload as bytes"""
        ...
    
    @property
    def key(self) -> Optional[bytes]:
        """Optional message key"""
        ...
    
    @property
    def offset(self) -> int:
        """Message offset in partition"""
        ...
    
    @property
    def timestamp(self) -> int:
        """Unix timestamp in milliseconds"""
        ...
    
    @property
    def partition(self) -> int:
        """Partition ID"""
        ...
    
    @property
    def topic(self) -> str:
        """Topic name"""
        ...
    
    def value_str(self) -> str:
        """Get value as UTF-8 string"""
        ...
    
    def key_str(self) -> Optional[str]:
        """Get key as UTF-8 string"""
        ...

class Producer:
    """Producer for publishing messages to a topic"""
    
    async def send(
        self,
        value: bytes,
        key: Optional[bytes] = None,
    ) -> int:
        """Send a message and return its offset"""
        ...
    
    async def send_to_partition(
        self,
        value: bytes,
        partition: int,
        key: Optional[bytes] = None,
    ) -> int:
        """Send a message to a specific partition"""
        ...
    
    async def send_batch(
        self,
        values: List[bytes],
        keys: Optional[List[Optional[bytes]]] = None,
    ) -> List[int]:
        """Send multiple messages and return their offsets"""
        ...

class Consumer:
    """Consumer for reading messages from a topic"""
    
    async def fetch(
        self,
        max_messages: int = 100,
        timeout_ms: int = 5000,
    ) -> List[Message]:
        """Fetch a batch of messages"""
        ...
    
    async def commit(self) -> None:
        """Commit current offsets"""
        ...
    
    async def seek(self, partition: int, offset: int) -> None:
        """Seek to a specific offset in a partition"""
        ...
    
    async def seek_to_beginning(self, partition: int) -> None:
        """Seek to the beginning of a partition"""
        ...
    
    async def seek_to_end(self, partition: int) -> None:
        """Seek to the end of a partition"""
        ...
    
    async def get_offset_bounds(self, partition: int) -> Tuple[int, int]:
        """Get the earliest and latest offsets for a partition"""
        ...
    
    def __aiter__(self) -> AsyncIterator[Message]:
        """Async iterator over messages"""
        ...
    
    async def __anext__(self) -> Message:
        """Get next message"""
        ...

class RivvenClient:
    """Main client for interacting with Rivven"""
    
    # Topic Management
    
    async def create_topic(
        self,
        name: str,
        partitions: int = 1,
        replication_factor: int = 1,
    ) -> None:
        """Create a new topic"""
        ...
    
    async def list_topics(self) -> List[str]:
        """List all topics"""
        ...
    
    async def delete_topic(self, name: str) -> None:
        """Delete a topic"""
        ...
    
    async def get_metadata(self, topic: str) -> Any:
        """Get detailed topic metadata"""
        ...
    
    # Producer/Consumer Factory
    
    def producer(self, topic: str) -> Producer:
        """Get a producer for the specified topic"""
        ...
    
    def consumer(
        self,
        topic: str,
        group_id: Optional[str] = None,
        auto_commit: bool = True,
    ) -> Consumer:
        """Get a consumer for the specified topic"""
        ...
    
    # Consumer Groups
    
    async def list_groups(self) -> List[str]:
        """List all consumer groups"""
        ...
    
    async def describe_group(self, group_id: str) -> Any:
        """Get details about a consumer group"""
        ...
    
    async def delete_group(self, group_id: str) -> None:
        """Delete a consumer group"""
        ...
    
    async def commit_offset(
        self,
        topic: str,
        partition: int,
        offset: int,
        group_id: str,
    ) -> None:
        """Commit an offset for a consumer group"""
        ...
    
    async def get_offset(
        self,
        topic: str,
        partition: int,
        group_id: str,
    ) -> int:
        """Get the committed offset for a consumer group"""
        ...
    
    # Direct Access (convenience methods)
    
    async def publish(
        self,
        topic: str,
        value: bytes,
        key: Optional[bytes] = None,
    ) -> int:
        """Publish a single message and return its offset"""
        ...
    
    async def consume(
        self,
        topic: str,
        max_messages: int = 100,
        timeout_ms: int = 5000,
    ) -> List[Message]:
        """Consume messages from a topic"""
        ...
    
    # Schema Registry
    
    async def register_schema(
        self,
        subject: str,
        schema: str,
    ) -> int:
        """Register a schema and return its ID"""
        ...
    
    async def get_schema(self, schema_id: int) -> str:
        """Get a schema by ID"""
        ...
    
    # Health
    
    async def ping(self) -> bool:
        """Check if the server is reachable"""
        ...

# Module-level functions

async def connect(
    addr: str,
    timeout_ms: int = 5000,
) -> RivvenClient:
    """
    Connect to a Rivven server.
    
    Args:
        addr: Server address (e.g., "localhost:9092")
        timeout_ms: Connection timeout in milliseconds
    
    Returns:
        RivvenClient instance
    
    Raises:
        RivvenError: If connection fails
    """
    ...

async def connect_tls(
    addr: str,
    ca_cert: str,
    client_cert: Optional[str] = None,
    client_key: Optional[str] = None,
    timeout_ms: int = 5000,
) -> RivvenClient:
    """
    Connect to a Rivven server with TLS.
    
    Args:
        addr: Server address (e.g., "localhost:9093")
        ca_cert: Path to CA certificate file
        client_cert: Optional path to client certificate file
        client_key: Optional path to client key file
        timeout_ms: Connection timeout in milliseconds
    
    Returns:
        RivvenClient instance
    
    Raises:
        RivvenError: If connection fails
    """
    ...

def version() -> str:
    """Return the SDK version string"""
    ...
