//! Error types for the `cdk-iroh` crate.

/// Errors that can occur in the Iroh mint server.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An I/O error occurred on the QUIC stream.
    #[error("I/O error: {0}")]
    Io(String),

    /// JSON (de)serialisation failed.
    #[error("JSON error: {0}")]
    Json(String),

    /// The incoming message length prefix exceeds the allowed maximum.
    #[error("Message too large: {0} bytes")]
    MessageTooLarge(u32),

    /// The JSON-RPC method is not recognised.
    #[error("Unknown method: {0}")]
    UnknownMethod(String),

    /// Missing or invalid params for a method.
    #[error("Invalid params: {0}")]
    InvalidParams(String),

    /// An error returned by the CDK mint.
    #[error("Mint error: {0}")]
    Mint(String),

    /// An error from the Iroh endpoint/transport layer.
    #[error("Iroh error: {0}")]
    Iroh(String),

    /// Failed to load or persist the node identity key.
    #[error("Identity error: {0}")]
    Identity(String),
}

impl From<cdk::Error> for Error {
    fn from(e: cdk::Error) -> Self {
        Error::Mint(e.to_string())
    }
}
