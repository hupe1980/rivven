"""Rivven Python SDK - High-performance streaming client"""

from rivven._rivven import (
    RivvenClient,
    Message,
    Consumer,
    Producer,
    RivvenError,
    connect,
    version,
)

__all__ = [
    "RivvenClient",
    "Message",
    "Consumer",
    "Producer",
    "RivvenError",
    "connect",
    "version",
]

# TLS support is optional (requires tls feature in Rust build)
try:
    from rivven._rivven import connect_tls

    __all__.append("connect_tls")
except ImportError:
    connect_tls = None  # type: ignore

__version__ = version()
