"""Rivven Python SDK - High-performance streaming client"""

from rivven._rivven import (
    RivvenClient,
    Message,
    Consumer,
    Producer,
    RivvenError,
    connect,
    connect_tls,
    version,
)

__all__ = [
    "RivvenClient",
    "Message",
    "Consumer",
    "Producer",
    "RivvenError",
    "connect",
    "connect_tls",
    "version",
]

__version__ = version()
