"""Tests for rivven Python SDK"""

import re

import pytest

import rivven
from rivven import Message, RivvenError, version


class TestMessage:
    """Test Message class"""

    def test_message_creation(self):
        """Test creating a message"""
        msg = Message(
            value=b"hello",
            key=b"key1",
            offset=42,
            timestamp=1234567890,
            partition=0,
            topic="test-topic",
        )
        assert msg.value == b"hello"
        assert msg.key == b"key1"
        assert msg.offset == 42

    def test_message_properties(self):
        """Test message properties are accessible"""
        msg = Message(
            value=b"test value",
            key=None,
            offset=100,
            timestamp=9999999999,
            partition=2,
            topic="my-topic",
        )
        assert msg.partition == 2
        assert msg.topic == "my-topic"
        assert msg.timestamp == 9999999999
        assert msg.key is None

    def test_message_value_str(self):
        """Test value_str() helper"""
        msg = Message(
            value=b"hello world",
            key=b"key",
            offset=0,
            timestamp=0,
            partition=0,
            topic="t",
        )
        assert msg.value_str() == "hello world"

    def test_message_key_str(self):
        """Test key_str() helper"""
        msg = Message(
            value=b"v",
            key=b"my-key",
            offset=0,
            timestamp=0,
            partition=0,
            topic="t",
        )
        assert msg.key_str() == "my-key"

    def test_message_key_str_none(self):
        """Test key_str() returns None when key is None"""
        msg = Message(
            value=b"v",
            key=None,
            offset=0,
            timestamp=0,
            partition=0,
            topic="t",
        )
        assert msg.key_str() is None


class TestRivvenError:
    """Test RivvenError exception"""

    def test_error_is_exception(self):
        """Test RivvenError is an Exception"""
        assert issubclass(RivvenError, Exception)

    def test_error_str(self):
        """Test error string representation"""
        # RivvenError is created by the Rust library, not directly
        # We test it through connection errors
        pass


@pytest.mark.asyncio
class TestConnection:
    """Test connection functions"""

    async def test_connect_invalid_address(self):
        """Test connection to invalid address fails"""
        with pytest.raises(RivvenError):
            await rivven.connect("invalid:9999")

    async def test_connect_refused(self):
        """Test connection refused to closed port"""
        with pytest.raises(RivvenError):
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
        assert hasattr(rivven, "RivvenError")
        assert hasattr(rivven, "connect")
        assert hasattr(rivven, "version")

    def test_all_list(self):
        """Test __all__ contains expected items"""
        expected = [
            "RivvenClient",
            "Message",
            "Consumer",
            "Producer",
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
