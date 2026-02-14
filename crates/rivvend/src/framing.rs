//! Shared connection framing for length-prefixed wire messages
//!
//! Both `secure_server` and `cluster_server` use an identical read loop:
//! 4-byte big-endian length prefix → validate size → read body → parse via
//! `Request::from_wire()`. This module extracts that pattern into a reusable
//! codec so bugs are fixed in one place (F-078).

use bytes::BytesMut;
use rivven_protocol::{Request, Response, WireFormat};
use std::io;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::{debug, warn};

/// Outcome of reading one framed message from the wire.
pub enum ReadFrame {
    /// Successfully parsed a request.
    Request {
        request: Request,
        wire_format: WireFormat,
        correlation_id: u32,
    },
    /// Client disconnected gracefully (EOF on length prefix).
    Disconnected,
    /// Timeout waiting for length prefix or body.
    Timeout,
}

/// Read one length-prefixed request from `stream`.
///
/// On size-exceeded or parse-failure an error response is written back
/// inline and `Ok(None)` is returned so the caller can `continue` the loop.
pub async fn read_framed_request<S>(
    stream: &mut S,
    buffer: &mut BytesMut,
    max_message_size: usize,
    read_timeout: Duration,
    peer_label: &str,
) -> anyhow::Result<Option<ReadFrame>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Read 4-byte length prefix with timeout
    let mut len_buf = [0u8; 4];
    match tokio::time::timeout(read_timeout, stream.read_exact(&mut len_buf)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
            return Ok(Some(ReadFrame::Disconnected));
        }
        Ok(Err(e)) => return Err(e.into()),
        Err(_) => {
            debug!("Read timeout from {} — closing connection", peer_label);
            return Ok(Some(ReadFrame::Timeout));
        }
    }

    let msg_len = u32::from_be_bytes(len_buf) as usize;

    // Validate message size
    if msg_len > max_message_size {
        warn!(
            "Message too large from {}: {} bytes (max: {})",
            peer_label, msg_len, max_message_size
        );
        let response = Response::Error {
            message: format!("MESSAGE_TOO_LARGE: {} bytes exceeds limit", msg_len),
        };
        send_response(stream, &response, WireFormat::Postcard, 0).await?;
        return Ok(None); // caller should continue
    }

    // Read message body with timeout to prevent slow-read DoS
    buffer.clear();
    buffer.resize(msg_len, 0);
    match tokio::time::timeout(read_timeout, stream.read_exact(buffer)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => return Err(e.into()),
        Err(_) => {
            debug!(
                "Read timeout during message body from {} — closing connection",
                peer_label
            );
            return Ok(Some(ReadFrame::Timeout));
        }
    }

    // Parse request with wire format detection
    match Request::from_wire(buffer) {
        Ok((request, wire_format, correlation_id)) => Ok(Some(ReadFrame::Request {
            request,
            wire_format,
            correlation_id,
        })),
        Err(e) => {
            warn!("Invalid request from {}: {}", peer_label, e);
            let detected_format = buffer
                .first()
                .and_then(|b| WireFormat::from_byte(*b))
                .unwrap_or(WireFormat::Postcard);
            let response = Response::Error {
                message: format!("INVALID_REQUEST: {}", e),
            };
            send_response(stream, &response, detected_format, 0).await?;
            Ok(None)
        }
    }
}

/// Write a length-prefixed response in the given wire format.
pub async fn send_response<S>(
    stream: &mut S,
    response: &Response,
    format: WireFormat,
    correlation_id: u32,
) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let response_bytes = response.to_wire(format, correlation_id)?;
    let len = response_bytes.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(&response_bytes).await?;
    stream.flush().await?;
    Ok(())
}
