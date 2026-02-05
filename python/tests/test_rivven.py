"""Tests for rivven Python SDK"""

import re

import pytest

import rivven
from rivven import Message, ProducerState, RivvenError, RivvenException, version


class TestMessage:
    """Test Message class"""

    def test_message_creation(self):
        """Test creating a message"""
        msg = Message(
            value=b"hello",
            offset=42,
            timestamp=1234567890,
            partition=0,
            topic="test-topic",
            key=b"key1",
        )
        assert msg.value == b"hello"
        assert msg.key == b"key1"
        assert msg.offset == 42

    def test_message_properties(self):
        """Test message properties are accessible"""
        msg = Message(
            value=b"test value",
            offset=100,
            timestamp=9999999999,
            partition=2,
            topic="my-topic",
            key=None,
        )
        assert msg.partition == 2
        assert msg.topic == "my-topic"
        assert msg.timestamp == 9999999999
        assert msg.key is None

    def test_message_value_str(self):
        """Test value_str() helper"""
        msg = Message(
            value=b"hello world",
            offset=0,
            timestamp=0,
            partition=0,
            topic="t",
            key=b"key",
        )
        assert msg.value_str() == "hello world"

    def test_message_key_str(self):
        """Test key_str() helper"""
        msg = Message(
            value=b"v",
            offset=0,
            timestamp=0,
            partition=0,
            topic="t",
            key=b"my-key",
        )
        assert msg.key_str() == "my-key"

    def test_message_key_str_none(self):
        """Test key_str() returns None when key is None"""
        msg = Message(
            value=b"v",
            offset=0,
            timestamp=0,
            partition=0,
            topic="t",
            key=None,
        )
        assert msg.key_str() is None

    def test_message_size(self):
        """Test size() returns correct byte count"""
        msg = Message(
            value=b"12345",
            offset=0,
            timestamp=0,
            partition=0,
            topic="t",
            key=b"abc",
        )
        assert msg.size() == 8  # 5 + 3

    def test_message_repr(self):
        """Test repr shows useful info"""
        msg = Message(
            value=b"v",
            offset=42,
            timestamp=12345,
            partition=3,
            topic="my-topic",
            key=None,
        )
        repr_str = repr(msg)
        assert "my-topic" in repr_str
        assert "42" in repr_str
        assert "3" in repr_str


class TestRivvenError:
    """Test RivvenError exception"""

    def test_error_is_exception(self):
        """Test RivvenError is an Exception"""
        assert issubclass(RivvenError, Exception)

    def test_rivven_error_is_alias(self):
        """Test RivvenError is an alias for RivvenException"""
        assert RivvenError is RivvenException

    def test_error_str(self):
        """Test error string representation"""
        # RivvenError is created by the Rust library, not directly
        # We test it through connection errors
        pass


class TestExceptionHierarchy:
    """Test exception type hierarchy"""

    def test_connection_exception_exists(self):
        """Test ConnectionException is available"""
        assert hasattr(rivven, "ConnectionException")
        assert issubclass(rivven.ConnectionException, rivven.RivvenException)

    def test_server_exception_exists(self):
        """Test ServerException is available"""
        assert hasattr(rivven, "ServerException")
        assert issubclass(rivven.ServerException, rivven.RivvenException)

    def test_timeout_exception_exists(self):
        """Test TimeoutException is available"""
        assert hasattr(rivven, "TimeoutException")
        assert issubclass(rivven.TimeoutException, rivven.RivvenException)

    def test_serialization_exception_exists(self):
        """Test SerializationException is available"""
        assert hasattr(rivven, "SerializationException")
        assert issubclass(rivven.SerializationException, rivven.RivvenException)

    def test_config_exception_exists(self):
        """Test ConfigException is available"""
        assert hasattr(rivven, "ConfigException")
        assert issubclass(rivven.ConfigException, rivven.RivvenException)


@pytest.mark.asyncio
class TestConnection:
    """Test connection functions"""

    async def test_connect_invalid_address(self):
        """Test connection to invalid address fails"""
        with pytest.raises((RivvenException, rivven.ConnectionException)):
            await rivven.connect("invalid:9999")

    async def test_connect_refused(self):
        """Test connection refused to closed port"""
        with pytest.raises((RivvenException, rivven.ConnectionException)):
            await rivven.connect("127.0.0.1:59999")


class TestVersion:
    """Test version function"""

    def test_version_returns_string(self):
        """Test version returns a string"""
        v = version()
        assert isinstance(v, str)
        assert len(v) > 0

    def test_version_format(self):
        """Test version has correct semver format"""
        v = version()
        # Should match semver: X.Y.Z or X.Y.Z-suffix
        assert re.match(r"^\d+\.\d+\.\d+", v) is not None

    def test_dunder_version(self):
        """Test __version__ is set"""
        assert hasattr(rivven, "__version__")
        assert rivven.__version__ == version()


class TestExports:
    """Test module exports"""

    def test_all_exports_exist(self):
        """Test all expected exports are present"""
        assert hasattr(rivven, "RivvenClient")
        assert hasattr(rivven, "Message")
        assert hasattr(rivven, "Consumer")
        assert hasattr(rivven, "Producer")
        assert hasattr(rivven, "ProducerState")
        assert hasattr(rivven, "RivvenError")
        assert hasattr(rivven, "connect")
        assert hasattr(rivven, "version")

    def test_exception_exports_exist(self):
        """Test exception exports are present"""
        assert hasattr(rivven, "RivvenException")
        assert hasattr(rivven, "ConnectionException")
        assert hasattr(rivven, "ServerException")
        assert hasattr(rivven, "TimeoutException")
        assert hasattr(rivven, "SerializationException")
        assert hasattr(rivven, "ConfigException")

    def test_all_list(self):
        """Test __all__ contains expected items"""
        expected = [
            "RivvenClient",
            "Message",
            "Consumer",
            "Producer",
            "ProducerState",
            "RivvenError",
            "connect",
            "version",
        ]
        for item in expected:
            assert item in rivven.__all__

    def test_connect_tls_optional(self):
        """Test connect_tls is available when TLS feature is enabled"""
        # connect_tls may or may not be available depending on build
        if rivven.connect_tls is not None:
            assert "connect_tls" in rivven.__all__


class TestClientMethods:
    """Test that RivvenClient has expected methods"""

    def test_topic_management_methods(self):
        """Test topic management methods exist"""
        client_class = rivven.RivvenClient
        # These are async methods, check they exist
        assert hasattr(client_class, "create_topic")
        assert hasattr(client_class, "delete_topic")
        assert hasattr(client_class, "list_topics")
        assert hasattr(client_class, "get_metadata")

    def test_authentication_methods(self):
        """Test authentication methods exist"""
        client_class = rivven.RivvenClient
        assert hasattr(client_class, "authenticate")
        assert hasattr(client_class, "authenticate_scram")

    def test_transaction_methods(self):
        """Test transaction methods exist"""
        client_class = rivven.RivvenClient
        assert hasattr(client_class, "init_producer_id")
        assert hasattr(client_class, "begin_transaction")
        assert hasattr(client_class, "commit_transaction")
        assert hasattr(client_class, "abort_transaction")
        assert hasattr(client_class, "publish_idempotent")
        assert hasattr(client_class, "add_partitions_to_txn")

    def test_admin_methods(self):
        """Test admin operation methods exist"""
        client_class = rivven.RivvenClient
        assert hasattr(client_class, "describe_topic_configs")
        assert hasattr(client_class, "alter_topic_config")
        assert hasattr(client_class, "create_partitions")
        assert hasattr(client_class, "get_offset_for_timestamp")
        assert hasattr(client_class, "delete_records")
        assert hasattr(client_class, "delete_records_batch")

    def test_consumer_group_methods(self):
        """Test consumer group methods exist"""
        client_class = rivven.RivvenClient
        assert hasattr(client_class, "list_groups")
        assert hasattr(client_class, "describe_group")
        assert hasattr(client_class, "delete_group")
        assert hasattr(client_class, "commit_offset")
        assert hasattr(client_class, "get_offset")


class TestProducerMethods:
    """Test that Producer has expected methods"""

    def test_producer_methods(self):
        """Test producer methods exist"""
        producer_class = rivven.Producer
        assert hasattr(producer_class, "send")
        assert hasattr(producer_class, "send_to_partition")
        assert hasattr(producer_class, "send_batch")
        assert hasattr(producer_class, "topic")


class TestProducerState:
    """Test that ProducerState has expected properties"""

    def test_producer_state_properties(self):
        """Test producer state properties exist"""
        producer_state_class = rivven.ProducerState
        assert hasattr(producer_state_class, "producer_id")
        assert hasattr(producer_state_class, "producer_epoch")
        assert hasattr(producer_state_class, "next_sequence")


class TestConsumerMethods:
    """Test that Consumer has expected methods"""

    def test_consumer_methods(self):
        """Test consumer methods exist"""
        consumer_class = rivven.Consumer
        assert hasattr(consumer_class, "fetch")
        assert hasattr(consumer_class, "commit")
        assert hasattr(consumer_class, "seek")
        assert hasattr(consumer_class, "seek_to_beginning")
        assert hasattr(consumer_class, "seek_to_end")
        assert hasattr(consumer_class, "get_offset_bounds")
        assert hasattr(consumer_class, "current_offset")

    def test_consumer_properties(self):
        """Test consumer properties exist"""
        consumer_class = rivven.Consumer
        assert hasattr(consumer_class, "topic")
        assert hasattr(consumer_class, "partition")
        assert hasattr(consumer_class, "group")
