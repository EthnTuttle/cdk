//! Iroh QUIC transport server for a Cashu mint.
//!
//! The server wraps an [`Arc<Mint>`] and exposes it over Iroh QUIC using the
//! `cashu/mint/0` ALPN.  Each inbound bidirectional stream carries exactly one
//! JSON-RPC 2.0 request/response pair (except for long-lived subscription
//! streams, which are stubbed with `todo!()` for now).

use std::path::PathBuf;
use std::sync::Arc;

use cdk::mint::Mint;
use futures::future::BoxFuture;
use iroh::endpoint::Connection;
use iroh::protocol::{ProtocolHandler, Router};
use iroh::{Endpoint, NodeId, SecretKey};

use crate::error::Error;
use crate::protocol::{methods, CASHU_ALPN};
use crate::wire::{read_message, write_message, RpcRequest, RpcResponse};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the [`IrohMintServer`].
#[derive(Debug, Default)]
pub struct IrohMintConfig {
    /// Optional path to persist the Ed25519 node identity key (raw 32 bytes).
    ///
    /// If `None`, a fresh random key is generated on every start (the node ID
    /// will change between restarts).  Supply a path to get a stable identity.
    pub identity_path: Option<PathBuf>,
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

/// An Iroh QUIC transport server for a Cashu mint.
#[derive(Debug)]
pub struct IrohMintServer {
    /// The underlying Iroh router (accept loop).
    router: Router,
    /// The node ID derived from the endpoint's secret key.
    node_id: NodeId,
}

impl IrohMintServer {
    /// Create a new server, bind the Iroh endpoint, and register the ALPN.
    pub async fn new(mint: Arc<Mint>, config: IrohMintConfig) -> Result<Self, Error> {
        // Load or generate the Ed25519 identity key.
        let secret_key = load_or_generate_key(config.identity_path.as_deref())?;
        let node_id = secret_key.public();

        // Build the Iroh endpoint.
        let endpoint = Endpoint::builder()
            .secret_key(secret_key)
            .alpns(vec![CASHU_ALPN.to_vec()])
            .bind()
            .await
            .map_err(|e| Error::Iroh(e.to_string()))?;

        // Build the Iroh protocol router.
        let handler = CashuHandler { mint };
        let router = Router::builder(endpoint)
            .accept(CASHU_ALPN, handler)
            .spawn();

        Ok(Self { router, node_id })
    }

    /// Returns the Iroh node ID of this server.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Returns the full [`iroh::NodeAddr`] for this server, including direct socket
    /// addresses.
    ///
    /// This is useful for local tests where the client and server are on the same
    /// machine and there is no relay server to discover the server's addresses through.
    /// Pass the returned `NodeAddr` to the client endpoint via
    /// `Endpoint::add_node_addr` before attempting to connect.
    pub async fn node_addr(&self) -> Result<iroh::NodeAddr, Error> {
        self.router
            .endpoint()
            .node_addr()
            .await
            .map_err(|e| Error::Iroh(e.to_string()))
    }

    /// Returns a human-readable `iroh://<z32-node-id>` URL.
    pub fn iroh_url(&self) -> String {
        format!("iroh://{}", self.node_id)
    }

    /// Run the server until it is shut down.
    ///
    /// This blocks until a graceful shutdown is requested (e.g. via
    /// [`IrohMintServer::shutdown`]) or until an unrecoverable error occurs.
    pub async fn run(self) -> Result<(), Error> {
        // The router's accept loop is already running as a background task;
        // we simply wait for it to finish (which happens on shutdown).
        self.router
            .shutdown()
            .await
            .map_err(|e| Error::Iroh(e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// Protocol handler
// ---------------------------------------------------------------------------

/// The Iroh [`ProtocolHandler`] implementation for the Cashu mint.
#[derive(Debug, Clone)]
struct CashuHandler {
    mint: Arc<Mint>,
}

impl ProtocolHandler for CashuHandler {
    fn accept(&self, connection: Connection) -> BoxFuture<'static, anyhow::Result<()>> {
        let mint = Arc::clone(&self.mint);
        Box::pin(async move {
            accept_connection(mint, connection).await?;
            Ok(())
        })
    }
}

/// Accept a single connection and drive all its bidirectional streams.
async fn accept_connection(mint: Arc<Mint>, connection: Connection) -> Result<(), Error> {
    tracing::debug!(
        remote_node = %connection.remote_node_id()
            .map(|id| id.to_string())
            .unwrap_or_else(|_| "<unknown>".to_string()),
        "accepted connection"
    );

    loop {
        match connection.accept_bi().await {
            Ok((send, recv)) => {
                let mint = Arc::clone(&mint);
                tokio::spawn(async move {
                    if let Err(e) = handle_stream(mint, send, recv).await {
                        tracing::warn!("stream error: {e}");
                    }
                });
            }
            Err(e) => {
                // Connection closed or error — stop accepting streams.
                tracing::debug!("connection closed: {e}");
                break;
            }
        }
    }

    Ok(())
}

/// Read one JSON-RPC request from `recv`, dispatch it, write the response to `send`.
async fn handle_stream(
    mint: Arc<Mint>,
    mut send: iroh::endpoint::SendStream,
    mut recv: iroh::endpoint::RecvStream,
) -> Result<(), Error> {
    let raw = match read_message(&mut recv).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("failed to read request: {e}");
            // Try to send a parse-error response before giving up.
            let err_resp = RpcResponse::err(
                serde_json::Value::Null,
                -32700,
                format!("parse error: {e}"),
            );
            let val = serde_json::to_value(&err_resp)
                .unwrap_or_else(|_| serde_json::Value::Null);
            let _ = write_message(&mut send, &val).await;
            return Err(e);
        }
    };

    // Deserialise the JSON-RPC 2.0 envelope.
    let req: RpcRequest = match serde_json::from_value(raw) {
        Ok(r) => r,
        Err(e) => {
            let err_resp = RpcResponse::err(
                serde_json::Value::Null,
                -32600,
                format!("invalid request: {e}"),
            );
            let val = serde_json::to_value(&err_resp)
                .unwrap_or_else(|_| serde_json::Value::Null);
            let _ = write_message(&mut send, &val).await;
            return Err(Error::Json(e.to_string()));
        }
    };

    tracing::debug!(method = %req.method, "dispatching request");

    // cashu_subscribe opens a long-lived bidi stream: hand it off to the
    // subscription handler which drives the stream for its entire lifetime.
    if req.method == methods::SUBSCRIBE {
        return crate::subscription::handle_subscription_stream(
            mint,
            send,
            recv,
            req.id,
        )
        .await;
    }

    let resp = crate::handler::dispatch(&mint, req).await;
    let val = serde_json::to_value(&resp).map_err(|e| Error::Json(e.to_string()))?;
    write_message(&mut send, &val).await?;

    // Finish the send side so the peer knows the response is complete.
    send.finish().map_err(|e| Error::Io(e.to_string()))?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Key helpers
// ---------------------------------------------------------------------------

/// Load a [`SecretKey`] from `path` (raw 32-byte LE file), or generate a new
/// one if the file does not exist.  If `path` is `None`, always generates.
fn load_or_generate_key(path: Option<&std::path::Path>) -> Result<SecretKey, Error> {
    match path {
        None => {
            let key = SecretKey::generate(rand::rngs::OsRng);
            Ok(key)
        }
        Some(p) => {
            if p.exists() {
                let bytes = std::fs::read(p)
                    .map_err(|e| Error::Identity(format!("read {}: {e}", p.display())))?;
                if bytes.len() != 32 {
                    return Err(Error::Identity(format!(
                        "identity file {} has wrong length (expected 32, got {})",
                        p.display(),
                        bytes.len()
                    )));
                }
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&bytes);
                Ok(SecretKey::from_bytes(&arr))
            } else {
                let key = SecretKey::generate(rand::rngs::OsRng);
                // Persist the key.
                if let Some(parent) = p.parent() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        Error::Identity(format!(
                            "create dir {}: {e}",
                            parent.display()
                        ))
                    })?;
                }
                std::fs::write(p, key.to_bytes()).map_err(|e| {
                    Error::Identity(format!("write {}: {e}", p.display()))
                })?;
                Ok(key)
            }
        }
    }
}
