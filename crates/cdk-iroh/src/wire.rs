//! Framing and JSON-RPC 2.0 envelope types for the Cashu/Iroh wire protocol.
//!
//! Each message is framed with a 4-byte big-endian length prefix followed by
//! a UTF-8 JSON body.  One QUIC bidirectional stream is used per
//! request/response pair.

use bytes::{BufMut, BytesMut};
use iroh::endpoint::{RecvStream, SendStream};
use serde::{Deserialize, Serialize};

use crate::error::Error;

/// Maximum allowed message size (16 MiB) to guard against malicious peers.
const MAX_MESSAGE_BYTES: u32 = 16 * 1024 * 1024;

/// Read a length-prefixed JSON message from `stream`.
///
/// Reads a 4-byte big-endian length, then that many bytes of JSON, and
/// deserialises the result into a [`serde_json::Value`].
pub async fn read_message(stream: &mut RecvStream) -> Result<serde_json::Value, Error> {
    // Read the 4-byte length prefix.
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| Error::Io(e.to_string()))?;
    let len = u32::from_be_bytes(len_buf);

    if len > MAX_MESSAGE_BYTES {
        return Err(Error::MessageTooLarge(len));
    }

    // Read the JSON body.
    let mut body = BytesMut::with_capacity(len as usize);
    body.resize(len as usize, 0);
    stream
        .read_exact(&mut body)
        .await
        .map_err(|e| Error::Io(e.to_string()))?;

    let value =
        serde_json::from_slice(&body).map_err(|e| Error::Json(e.to_string()))?;
    Ok(value)
}

/// Write a length-prefixed JSON message to `stream`.
pub async fn write_message(
    stream: &mut SendStream,
    value: &serde_json::Value,
) -> Result<(), Error> {
    let body = serde_json::to_vec(value).map_err(|e| Error::Json(e.to_string()))?;
    let len = body.len() as u32;

    let mut buf = BytesMut::with_capacity(4 + body.len());
    buf.put_u32(len);
    buf.extend_from_slice(&body);

    stream
        .write_all(&buf)
        .await
        .map_err(|e| Error::Io(e.to_string()))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// JSON-RPC 2.0 envelope types
// ---------------------------------------------------------------------------

/// A JSON-RPC 2.0 request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcRequest {
    /// Must be `"2.0"`.
    pub jsonrpc: String,
    /// Method name (e.g. `"cashu_getKeys"`).
    pub method: String,
    /// Request parameters (may be `null`).
    pub params: serde_json::Value,
    /// Request identifier echoed back in the response.
    pub id: serde_json::Value,
}

/// A JSON-RPC 2.0 response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcResponse {
    /// Always `"2.0"`.
    pub jsonrpc: String,
    /// Successful result, mutually exclusive with `error`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    /// Error object, mutually exclusive with `result`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
    /// Request identifier copied from the request.
    pub id: serde_json::Value,
}

impl RpcResponse {
    /// Construct a successful response.
    pub fn ok(id: serde_json::Value, result: serde_json::Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            result: Some(result),
            error: None,
            id,
        }
    }

    /// Construct an error response.
    pub fn err(id: serde_json::Value, code: i32, message: String) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            result: None,
            error: Some(RpcError { code, message }),
            id,
        }
    }
}

/// A JSON-RPC 2.0 error object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcError {
    /// Numeric error code.
    pub code: i32,
    /// Human-readable message.
    pub message: String,
}
