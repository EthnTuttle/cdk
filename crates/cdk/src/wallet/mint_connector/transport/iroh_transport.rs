//! Iroh QUIC transport for the Cashu wallet
//!
//! Connects wallets to Iroh-enabled mints by `NodeId` instead of URL,
//! using QUIC bidirectional streams with a 4-byte big-endian length-prefix
//! framing and a JSON-RPC 2.0 envelope.
//!
//! # Wire protocol constants
//! - ALPN: `b"cashu/mint/0"`
//! - Framing: 4-byte big-endian length prefix + JSON body per QUIC stream
//! - One stream per request/response pair

use std::sync::Arc;

use async_trait::async_trait;
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::{Endpoint, NodeId};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::Mutex;
use url::Url;

use super::super::Error;
use crate::wallet::mint_connector::transport::{ErrorResponse, Transport};

/// ALPN protocol identifier for the Cashu mint protocol.
pub const CASHU_ALPN: &[u8] = b"cashu/mint/0";

/// Extract the Iroh [`NodeId`] from a NUT-18 [`cdk_common::nuts::nut18::Transport`]
/// if its type is [`cdk_common::nuts::nut18::TransportType::Iroh`].
///
/// The `target` field of an Iroh transport is a z32-encoded NodeId string.
/// Returns `None` if the transport type is not `Iroh` or if parsing fails.
pub fn node_id_from_nut18_transport(
    transport: &cdk_common::nuts::nut18::Transport,
) -> Option<NodeId> {
    use cdk_common::nuts::nut18::TransportType;
    if transport._type == TransportType::Iroh {
        transport.target.parse().ok()
    } else {
        None
    }
}

/// Maximum framed message size (16 MiB).  Responses above this limit will be
/// rejected to defend against memory-exhaustion.
const MAX_FRAME_SIZE: u32 = 16 * 1024 * 1024;

/// Iroh QUIC transport for wallet-to-mint communication.
///
/// Maintains a cached QUIC connection to the remote mint node, reopening it
/// transparently when the connection is lost.  Each HTTP-equivalent request
/// is issued over a fresh QUIC bidirectional stream, satisfying the
/// one-stream-per-RPC contract required by the wire protocol.
///
/// # Clone & Send/Sync
/// `IrohAsync` wraps the connection cache in an `Arc<Mutex<…>>`, so cloning
/// is cheap and the handle can be shared across async tasks.
///
/// # Default
/// The `Default` impl creates a placeholder whose `endpoint` and `node_id`
/// fields are filled with sentinel values.  Any call to `http_get` or
/// `http_post` on the default instance will return an error, because there
/// is no real peer to connect to.  Use [`IrohAsync::new`] to construct a
/// properly initialised transport.
#[derive(Clone)]
pub struct IrohAsync {
    endpoint: Option<Endpoint>,
    node_id: Option<NodeId>,
    connection: Arc<Mutex<Option<Connection>>>,
}

impl std::fmt::Debug for IrohAsync {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IrohAsync")
            .field("node_id", &self.node_id.map(|n| n.to_string()))
            .field(
                "connection",
                &if self.connection.try_lock().map_or(false, |g| g.is_some()) {
                    "connected"
                } else {
                    "disconnected"
                },
            )
            .finish()
    }
}

impl Default for IrohAsync {
    /// Returns a placeholder transport with no peer configured.
    ///
    /// Any actual request will fail with [`Error::Custom`].  Use
    /// [`IrohAsync::new`] to obtain a functional instance.
    fn default() -> Self {
        Self {
            endpoint: None,
            node_id: None,
            connection: Arc::new(Mutex::new(None)),
        }
    }
}

impl IrohAsync {
    /// Create a new [`IrohAsync`] that dials `node_id` over `endpoint`.
    pub fn new(endpoint: Endpoint, node_id: NodeId) -> Self {
        Self {
            endpoint: Some(endpoint),
            node_id: Some(node_id),
            connection: Arc::new(Mutex::new(None)),
        }
    }

    /// Return the live connection to the peer, establishing one if necessary.
    async fn get_connection(&self) -> Result<Connection, Error> {
        let endpoint = self
            .endpoint
            .as_ref()
            .ok_or_else(|| Error::Custom("IrohAsync used without a configured endpoint. Use IrohAsync::new instead of IrohAsync::default.".into()))?;
        let node_id = self.node_id.ok_or_else(|| {
            Error::Custom("IrohAsync used without a configured node_id.".into())
        })?;

        let mut guard = self.connection.lock().await;

        // Re-use an existing healthy connection.
        if let Some(ref conn) = *guard {
            if conn.close_reason().is_none() {
                return Ok(conn.clone());
            }
            // Connection is closed; drop the cached handle and reconnect.
            *guard = None;
        }

        let conn = endpoint
            .connect(node_id, CASHU_ALPN)
            .await
            .map_err(|e| Error::Custom(format!("Iroh connect failed: {e}")))?;

        *guard = Some(conn.clone());
        Ok(conn)
    }

    /// Perform a complete JSON-RPC round-trip over a new bidirectional stream.
    async fn rpc<R>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<R, Error>
    where
        R: DeserializeOwned,
    {
        let conn = self.get_connection().await?;
        let (mut send, mut recv) = conn
            .open_bi()
            .await
            .map_err(|e| Error::Custom(format!("open_bi failed: {e}")))?;

        // Build the JSON-RPC 2.0 request envelope.
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1
        });
        let request_bytes =
            serde_json::to_vec(&request).map_err(|e| Error::Custom(e.to_string()))?;

        write_framed(&mut send, &request_bytes).await?;
        // Signal end-of-send so the server sees EOF on its recv side.
        send.finish()
            .map_err(|e| Error::Custom(format!("stream finish failed: {e}")))?;

        let response_bytes = read_framed(&mut recv).await?;

        // Parse the JSON-RPC response envelope.
        let envelope: serde_json::Value = serde_json::from_slice(&response_bytes)
            .map_err(|e| Error::Custom(format!("JSON parse error: {e}")))?;

        // Propagate a JSON-RPC error if present.
        if let Some(err_obj) = envelope.get("error") {
            return Err(Error::Custom(format!("JSON-RPC error: {err_obj}")));
        }

        let result = envelope
            .get("result")
            .ok_or_else(|| Error::Custom("JSON-RPC response missing 'result' field".into()))?;

        // Try to deserialise directly as the expected response type.  Fall back
        // to the Cashu error response format if that fails.
        let result_str = serde_json::to_string(result)
            .map_err(|e| Error::Custom(format!("result serialisation error: {e}")))?;

        serde_json::from_str::<R>(&result_str).map_err(|err| {
            tracing::warn!("Iroh response deserialisation error: {}", err);
            match ErrorResponse::from_json(&result_str) {
                Ok(ok) => <ErrorResponse as Into<Error>>::into(ok),
                Err(_) => Error::Custom(format!("deserialisation error: {err}")),
            }
        })
    }

    /// Open a long-lived NUT-17 subscription stream to the mint.
    ///
    /// This sends the initial `cashu_subscribe` JSON-RPC handshake and waits
    /// for the server's acknowledgement.  On success it returns the raw
    /// `(SendStream, RecvStream)` pair so the caller can drive the stream
    /// with NUT-17 `subscribe` / `unsubscribe` / notification traffic.
    ///
    /// # Wire protocol
    ///
    /// ```text
    /// wallet → server  :  {"jsonrpc":"2.0","method":"cashu_subscribe","params":{},"id":0}
    /// server → wallet  :  {"jsonrpc":"2.0","result":{"status":"OK"},"id":0}
    /// ```
    ///
    /// After the handshake the caller may send `WsRequest` frames and receive
    /// `WsMessageOrResponse` frames on the same streams.
    ///
    /// # Note
    /// Integration with the wallet's high-level subscription API is a follow-up
    /// task.  This method is exposed as a building-block giving direct access
    /// to the raw stream.
    pub async fn open_subscription_stream(
        &self,
    ) -> Result<(SendStream, RecvStream), Error> {
        let conn = self.get_connection().await?;
        let (mut send, mut recv) = conn
            .open_bi()
            .await
            .map_err(|e| Error::Custom(format!("open_bi failed: {e}")))?;

        // Send the cashu_subscribe handshake.
        let handshake = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "cashu_subscribe",
            "params": {},
            "id": 0
        });
        let bytes =
            serde_json::to_vec(&handshake).map_err(|e| Error::Custom(e.to_string()))?;
        write_framed(&mut send, &bytes).await?;
        // Do NOT call send.finish() — the stream must remain open.

        // Read and validate the server acknowledgement.
        let ack_bytes = read_framed(&mut recv).await?;
        let ack: serde_json::Value = serde_json::from_slice(&ack_bytes)
            .map_err(|e| Error::Custom(format!("subscribe ack parse error: {e}")))?;

        if let Some(err) = ack.get("error") {
            return Err(Error::Custom(format!(
                "cashu_subscribe rejected by server: {err}"
            )));
        }

        let status = ack
            .get("result")
            .and_then(|r| r.get("status"))
            .and_then(|s| s.as_str());
        if status != Some("OK") {
            return Err(Error::Custom(format!(
                "cashu_subscribe unexpected ack: {ack}"
            )));
        }

        Ok((send, recv))
    }
}

// ---------------------------------------------------------------------------
// URL → JSON-RPC method/params mapping
// ---------------------------------------------------------------------------

/// Extract the JSON-RPC method name and any path-derived params from a URL.
///
/// `is_post` indicates whether the call is a POST request (body params will be
/// merged in by the caller; this function only extracts path-segment params).
fn url_to_method(url: &Url, is_post: bool) -> Result<(&'static str, serde_json::Value), Error> {
    let path = url.path();
    // Strip any trailing slash for uniform matching.
    let path = path.trim_end_matches('/');

    // Ordered from most-specific to least-specific to avoid prefix collisions.
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    match segments.as_slice() {
        // GET /v1/keys
        ["v1", "keys"] if !is_post => Ok(("cashu_getKeys", serde_json::json!({}))),

        // GET /v1/keys/{id}
        ["v1", "keys", id] if !is_post => Ok((
            "cashu_getKeyById",
            serde_json::json!({ "id": *id }),
        )),

        // GET /v1/keysets
        ["v1", "keysets"] if !is_post => Ok(("cashu_getKeysets", serde_json::json!({}))),

        // GET /v1/mint/quote/bolt11/{id}
        ["v1", "mint", "quote", "bolt11", id] if !is_post => Ok((
            "cashu_getMintQuote",
            serde_json::json!({ "id": *id, "payment_method": "bolt11" }),
        )),

        // POST /v1/mint/quote/bolt11
        ["v1", "mint", "quote", "bolt11"] if is_post => Ok(("cashu_postMintQuote", serde_json::json!({}))),

        // POST /v1/mint/bolt11
        ["v1", "mint", "bolt11"] if is_post => Ok(("cashu_postMint", serde_json::json!({}))),

        // GET /v1/melt/quote/bolt11/{id}
        ["v1", "melt", "quote", "bolt11", id] if !is_post => Ok((
            "cashu_getMeltQuote",
            serde_json::json!({ "id": *id, "payment_method": "bolt11" }),
        )),

        // POST /v1/melt/quote/bolt11
        ["v1", "melt", "quote", "bolt11"] if is_post => Ok(("cashu_postMeltQuote", serde_json::json!({}))),

        // POST /v1/melt/bolt11
        ["v1", "melt", "bolt11"] if is_post => Ok(("cashu_postMelt", serde_json::json!({}))),

        // POST /v1/swap
        ["v1", "swap"] if is_post => Ok(("cashu_postSwap", serde_json::json!({}))),

        // POST /v1/checkstate
        ["v1", "checkstate"] if is_post => Ok(("cashu_checkState", serde_json::json!({}))),

        // GET /v1/info
        ["v1", "info"] if !is_post => Ok(("cashu_getInfo", serde_json::json!({}))),

        // POST /v1/restore
        ["v1", "restore"] if is_post => Ok(("cashu_restore", serde_json::json!({}))),

        _ => Err(Error::Custom(format!(
            "IrohAsync: unknown URL path '{}' (is_post={is_post})",
            url.path()
        ))),
    }
}

// ---------------------------------------------------------------------------
// Framing helpers
// ---------------------------------------------------------------------------

/// Write a length-prefixed frame: `[u32 big-endian length][data]`.
async fn write_framed(send: &mut SendStream, data: &[u8]) -> Result<(), Error> {
    let len = u32::try_from(data.len())
        .map_err(|_| Error::Custom("frame too large to encode as u32".into()))?;
    let len_bytes = len.to_be_bytes();
    send.write_all(&len_bytes)
        .await
        .map_err(|e| Error::Custom(format!("write length prefix failed: {e}")))?;
    send.write_all(data)
        .await
        .map_err(|e| Error::Custom(format!("write frame body failed: {e}")))?;
    Ok(())
}

/// Read a length-prefixed frame: `[u32 big-endian length][data]`.
async fn read_framed(recv: &mut RecvStream) -> Result<Vec<u8>, Error> {
    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf)
        .await
        .map_err(|e| Error::Custom(format!("read length prefix failed: {e}")))?;
    let len = u32::from_be_bytes(len_buf);
    if len > MAX_FRAME_SIZE {
        return Err(Error::Custom(format!(
            "frame too large: {len} bytes (limit {MAX_FRAME_SIZE})"
        )));
    }
    let mut buf = vec![0u8; len as usize];
    recv.read_exact(&mut buf)
        .await
        .map_err(|e| Error::Custom(format!("read frame body failed: {e}")))?;
    Ok(buf)
}

// ---------------------------------------------------------------------------
// NUT-06 discovery helpers
// ---------------------------------------------------------------------------

/// Parse an `iroh://` URL and return the encoded [`NodeId`].
///
/// The URL format is `iroh://<z32-encoded-node-id>` as produced by
/// `IrohMintServer::iroh_url()` on the mint side.
///
/// Returns `None` if the URL does not start with `iroh://` or the node-id
/// segment cannot be parsed.
pub fn parse_iroh_url(url: &str) -> Option<NodeId> {
    let node_id_str = url.strip_prefix("iroh://")?;
    node_id_str.parse().ok()
}

/// Scan a [`cdk_common::nuts::MintInfo`]'s `urls` field and return the first
/// Iroh [`NodeId`] found.
///
/// Returns `None` when no `iroh://` URL is present in the mint's info.
pub fn iroh_node_id_from_mint_info(info: &cdk_common::nuts::MintInfo) -> Option<NodeId> {
    info.urls.as_ref()?.iter().find_map(|u| parse_iroh_url(u))
}

// ---------------------------------------------------------------------------
// Transport trait implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl Transport for IrohAsync {
    fn with_proxy(
        &mut self,
        _proxy: Url,
        _host_matcher: Option<&str>,
        _accept_invalid_certs: bool,
    ) -> Result<(), Error> {
        Err(Error::Custom(
            "IrohAsync does not support HTTP proxies; Iroh uses its own relay infrastructure."
                .into(),
        ))
    }

    async fn http_get<R>(&self, url: Url, _auth: Option<cdk_common::AuthToken>) -> Result<R, Error>
    where
        R: DeserializeOwned,
    {
        let (method, params) = url_to_method(&url, false)?;
        self.rpc::<R>(method, params).await
    }

    async fn http_post<P, R>(
        &self,
        url: Url,
        _auth_token: Option<cdk_common::AuthToken>,
        payload: &P,
    ) -> Result<R, Error>
    where
        P: Serialize + ?Sized + Send + Sync,
        R: DeserializeOwned,
    {
        let (method, path_params) = url_to_method(&url, true)?;

        // Serialise the body payload into a JSON Value, then merge in any
        // path-derived params so the server receives a unified params object.
        let body_value =
            serde_json::to_value(payload).map_err(|e| Error::Custom(e.to_string()))?;

        let params = if path_params == serde_json::json!({}) {
            // No path params — use the body directly.
            body_value
        } else if let (Some(obj), Some(path_obj)) =
            (body_value.as_object(), path_params.as_object())
        {
            // Merge path params into the body object.
            let mut merged = obj.clone();
            for (k, v) in path_obj {
                merged.insert(k.clone(), v.clone());
            }
            serde_json::Value::Object(merged)
        } else {
            body_value
        };

        self.rpc::<R>(method, params).await
    }

    #[cfg(all(feature = "bip353", not(target_arch = "wasm32")))]
    async fn resolve_dns_txt(&self, _domain: &str) -> Result<Vec<String>, Error> {
        Err(Error::Custom(
            "IrohAsync does not support DNS TXT resolution; use the default HTTP transport for BIP-353 lookups.".into(),
        ))
    }
}
