"""Type stubs for rivven._rivven native module"""

from typing import Optional, List, Tuple, Any, AsyncIterator

# Exception hierarchy
class RivvenException(Exception):
    """Base exception for all Rivven errors"""
    ...

class ConnectionException(RivvenException):
    """Connection-related errors (network, pool exhausted, circuit breaker)"""
    ...

class ServerException(RivvenException):
    """Server-side errors"""
    ...

class TimeoutException(RivvenException):
    """Request timeout errors"""
    ...

class SerializationException(RivvenException):
    """Serialization/deserialization errors"""
    ...

class ConfigException(RivvenException):
    """Configuration errors"""
    ...

# Backward-compatible alias
RivvenError = RivvenException

class ProducerState:
    """State for exactly-once semantics producers.
    
    Contains producer_id, epoch, and sequence number tracking.
    Returned by init_producer_id() and required for transactional operations.
    """
    
    @property
    def producer_id(self) -> int:
        """Get the producer ID"""
        ...
    
    @property
    def producer_epoch(self) -> int:
        """Get the producer epoch"""
        ...
    
    @property
    def next_sequence(self) -> int:
        """Get and increment the next sequence number"""
        ...

class Message:
    """A message consumed from a Rivven topic"""
    
    def __init__(
        self,
        value: bytes,
        offset: int,
        timestamp: int,
        partition: int,
        topic: str,
        key: Optional[bytes] = None,
    ) -> None:
        """Create a new Message (primarily for internal use)"""
        ...
    
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
    
    def size(self) -> int:
        """Get total size of message payload (value + key) in bytes"""
        ...

class Producer:
    """Producer for publishing messages to a topic"""
    
    @property
    def topic(self) -> str:
        """Get the topic this producer is bound to"""
        ...
    
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
        messages: List[Tuple[bytes, Optional[bytes]]],
    ) -> List[int]:
        """Send multiple messages (value, key) and return their offsets"""
        ...

class Consumer:
    """Consumer for reading messages from a topic"""
    
    @property
    def topic(self) -> str:
        """Get the topic being consumed"""
        ...
    
    @property
    def partition(self) -> int:
        """Get the partition being consumed"""
        ...
    
    @property
    def group(self) -> Optional[str]:
        """Get the consumer group (if any)"""
        ...
    
    async def current_offset(self) -> int:
        """Get the current offset"""
        ...
    
    async def fetch(
        self,
        max_messages: Optional[int] = None,
    ) -> List[Message]:
        """Fetch a batch of messages"""
        ...
    
    async def commit(self) -> None:
        """Commit current offsets"""
        ...
    
    async def seek(self, offset: int) -> None:
        """Seek to a specific offset"""
        ...
    
    async def seek_to_beginning(self) -> None:
        """Seek to the beginning of the partition"""
        ...
    
    async def seek_to_end(self) -> None:
        """Seek to the end of the partition"""
        ...
    
    async def get_offset_bounds(self) -> Tuple[int, int]:
        """Get the earliest and latest offsets for the partition"""
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
    
    async def consumer(
        self,
        topic: str,
        partition: int = 0,
        group: Optional[str] = None,
        start_offset: Optional[int] = None,
        batch_size: int = 100,
        auto_commit: bool = True,
    ) -> Consumer:
        """Get a consumer for the specified topic"""
        ...
    
    # Authentication
    
    async def authenticate(self, username: str, password: str) -> Tuple[str, int]:
        """Authenticate with username/password. Returns (session_id, expires_in)"""
        ...
    
    async def authenticate_scram(self, username: str, password: str) -> Tuple[str, int]:
        """Authenticate with SCRAM-SHA-256. Returns (session_id, expires_in)"""
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
        group: str,
        topic: str,
        partition: int,
        offset: int,
    ) -> None:
        """Commit an offset for a consumer group"""
        ...
    
    async def get_offset(
        self,
        group: str,
        topic: str,
        partition: int,
    ) -> Optional[int]:
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
        partition: int,
        offset: int,
        max_messages: int,
    ) -> List[Message]:
        """Consume messages from a topic"""
        ...
    
    # Admin Operations
    
    async def get_offset_for_timestamp(
        self,
        topic: str,
        partition: int,
        timestamp_ms: int,
    ) -> Optional[int]:
        """Get the offset for a given timestamp"""
        ...
    
    async def describe_topic_configs(self, topic: str) -> dict[str, str]:
        """Get topic configuration as key-value pairs"""
        ...
    
    async def alter_topic_config(self, topic: str, key: str, value: str) -> None:
        """Alter a topic configuration value"""
        ...
    
    async def create_partitions(self, topic: str, new_total: int) -> int:
        """Add partitions to a topic. Returns new partition count."""
        ...
    
    async def delete_records(
        self,
        topic: str,
        partition: int,
        before_offset: int,
    ) -> int:
        """Delete records before an offset. Returns new low watermark."""
        ...
    
    async def delete_records_batch(
        self,
        topic: str,
        partition_offsets: List[Tuple[int, int]],
    ) -> List[Tuple[int, int, Optional[str]]]:
        """Delete records for multiple partitions.
        
        Args:
            topic: Topic name
            partition_offsets: List of (partition, before_offset) tuples
        
        Returns:
            List of (partition, low_watermark, error) tuples
        """
        ...
    
    # Transaction Support
    
    async def init_producer_id(
        self,
        transactional_id: Optional[str] = None,
    ) -> ProducerState:
        """Initialize producer for idempotent/transactional producing.
        
        Returns:
            ProducerState containing producer_id, epoch, and sequence tracking
        """
        ...
    
    async def publish_idempotent(
        self,
        topic: str,
        value: bytes,
        producer_state: ProducerState,
        key: Optional[bytes] = None,
    ) -> int:
        """Publish with exactly-once semantics. Returns offset."""
        ...
    
    async def begin_transaction(
        self,
        transactional_id: str,
        producer_state: ProducerState,
    ) -> None:
        """Begin a transaction"""
        ...
    
    async def add_partitions_to_txn(
        self,
        transactional_id: str,
        producer_state: ProducerState,
        topic_partitions: List[Tuple[str, int]],
    ) -> None:
        """Add topic-partitions to a transaction"""
        ...
    
    async def commit_transaction(
        self,
        transactional_id: str,
        producer_state: ProducerState,
    ) -> None:
        """Commit a transaction"""
        ...
    
    async def abort_transaction(
        self,
        transactional_id: str,
        producer_state: ProducerState,
    ) -> None:
        """Abort a transaction"""
        ...
    
    # Health
    
    async def ping(self) -> None:
        """Check if the server is reachable"""
        ...

# Module-level functions

async def connect(addr: str) -> RivvenClient:
    """
    Connect to a Rivven server.
    
    Args:
        addr: Server address (e.g., "localhost:9092")
    
    Returns:
        RivvenClient instance
    
    Raises:
        RivvenError: If connection fails
    """
    ...

async def connect_tls(
    addr: str,
    ca_cert_path: str,
    server_name: str,
    client_cert_path: Optional[str] = None,
    client_key_path: Optional[str] = None,
) -> RivvenClient:
    """
    Connect to a Rivven server with TLS.
    
    Args:
        addr: Server address (e.g., "localhost:9093")
        ca_cert_path: Path to CA certificate file
        server_name: Server hostname for certificate verification
        client_cert_path: Optional path to client certificate file
        client_key_path: Optional path to client key file
    
    Returns:
        RivvenClient instance
    
    Raises:
        RivvenError: If connection fails
    """
    ...

def version() -> str:
    """Return the SDK version string"""
    ...
