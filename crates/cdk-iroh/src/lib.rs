//! Iroh QUIC transport server for a Cashu mint.
//!
//! This crate provides [`IrohMintServer`], which exposes an [`Arc<Mint>`]
//! over [Iroh](https://iroh.computer/) peer-to-peer QUIC connections.
//!
//! ## Wire protocol
//!
//! - **ALPN**: `b"cashu/mint/0"` ([`CASHU_ALPN`])
//! - **Framing**: 4-byte big-endian length prefix + JSON body per QUIC
//!   bidirectional stream
//! - **Envelope**: JSON-RPC 2.0 ([`wire::RpcRequest`] / [`wire::RpcResponse`])
//! - **One stream per request/response pair** (subscriptions are long-lived
//!   and currently stubbed)

#![warn(missing_docs)]

pub(crate) mod error;
pub(crate) mod handler;
pub mod protocol;
pub(crate) mod server;
pub(crate) mod subscription;
pub mod wire;

pub use error::Error;
pub use protocol::CASHU_ALPN;
pub use protocol::methods;
pub use server::{IrohMintConfig, IrohMintServer};
pub use wire::{RpcError, RpcRequest, RpcResponse};
