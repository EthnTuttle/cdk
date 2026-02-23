//! Wire protocol constants for the Cashu/Iroh transport.

/// ALPN identifier for the Cashu mint protocol over Iroh QUIC.
pub const CASHU_ALPN: &[u8] = b"cashu/mint/0";

/// JSON-RPC method name constants.
pub mod methods {
    /// Get active mint keys (NUT-01)
    pub const GET_KEYS: &str = "cashu_getKeys";
    /// Get all keysets (NUT-02)
    pub const GET_KEYSETS: &str = "cashu_getKeysets";
    /// Get keys for a specific keyset by ID (NUT-01)
    pub const GET_KEY_BY_ID: &str = "cashu_getKeyById";
    /// Swap proofs for new blinded signatures (NUT-03)
    pub const POST_SWAP: &str = "cashu_postSwap";
    /// Get a mint quote (NUT-04)
    pub const GET_MINT_QUOTE: &str = "cashu_getMintQuote";
    /// Check a mint quote status (NUT-04)
    pub const POST_MINT_QUOTE: &str = "cashu_postMintQuote";
    /// Mint tokens against a paid quote (NUT-04)
    pub const POST_MINT: &str = "cashu_postMint";
    /// Get a melt quote (NUT-05)
    pub const GET_MELT_QUOTE: &str = "cashu_getMeltQuote";
    /// Check a melt quote status (NUT-05)
    pub const POST_MELT_QUOTE: &str = "cashu_postMeltQuote";
    /// Melt tokens (NUT-05)
    pub const POST_MELT: &str = "cashu_postMelt";
    /// Check proof state (NUT-07)
    pub const CHECK_STATE: &str = "cashu_checkState";
    /// Get mint info (NUT-06)
    pub const GET_INFO: &str = "cashu_getInfo";
    /// Restore blind signatures (NUT-09)
    pub const RESTORE: &str = "cashu_restore";
    /// Subscribe to notifications (NUT-17, long-lived stream)
    pub const SUBSCRIBE: &str = "cashu_subscribe";
}
