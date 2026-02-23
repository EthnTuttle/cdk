//! Request dispatch: maps incoming JSON-RPC method names to mint operations.

use std::sync::Arc;

use cdk::mint::{Mint, QuoteId};
use cdk::nuts::{
    CheckStateRequest, Id, MeltQuoteBolt11Request, MeltRequest, MintRequest, RestoreRequest,
    SwapRequest,
};
use cdk::util::unix_time;
use cdk_common::melt::MeltQuoteRequest;

use crate::protocol::methods;
use crate::wire::{RpcRequest, RpcResponse};

/// JSON-RPC error codes (application-level).
const RPC_INVALID_PARAMS: i32 = -32602;
const RPC_METHOD_NOT_FOUND: i32 = -32601;
const RPC_INTERNAL_ERROR: i32 = -32603;

/// Dispatch a validated `RpcRequest` to the appropriate mint handler.
///
/// This function is infallible at the transport level: all errors are
/// represented as JSON-RPC error responses so the caller can always send a
/// response back to the peer.
pub async fn dispatch(mint: &Arc<Mint>, req: RpcRequest) -> RpcResponse {
    let id = req.id.clone();
    match req.method.as_str() {
        methods::GET_KEYS => handle_get_keys(mint, id).await,
        methods::GET_KEYSETS => handle_get_keysets(mint, id).await,
        methods::GET_KEY_BY_ID => handle_get_key_by_id(mint, req.params, id).await,
        methods::POST_SWAP => handle_post_swap(mint, req.params, id).await,
        methods::GET_MINT_QUOTE => handle_get_mint_quote(mint, req.params, id).await,
        methods::POST_MINT_QUOTE => handle_post_mint_quote(mint, req.params, id).await,
        methods::POST_MINT => handle_post_mint(mint, req.params, id).await,
        methods::GET_MELT_QUOTE => handle_get_melt_quote(mint, req.params, id).await,
        methods::POST_MELT_QUOTE => handle_post_melt_quote(mint, req.params, id).await,
        methods::POST_MELT => handle_post_melt(mint, req.params, id).await,
        methods::CHECK_STATE => handle_check_state(mint, req.params, id).await,
        methods::GET_INFO => handle_get_info(mint, id).await,
        methods::RESTORE => handle_restore(mint, req.params, id).await,
        methods::SUBSCRIBE => {
            // cashu_subscribe is handled before dispatch() in server.rs by
            // routing the stream to subscription::handle_subscription_stream.
            // If we somehow reach here, it's a programming error.
            unreachable!("cashu_subscribe must be handled before reaching dispatch()");
        }
        other => RpcResponse::err(
            id,
            RPC_METHOD_NOT_FOUND,
            format!("unknown method: {other}"),
        ),
    }
}

// ---------------------------------------------------------------------------
// Helper macros / functions
// ---------------------------------------------------------------------------

/// Deserialise params and convert errors into a JSON-RPC error response.
macro_rules! parse_params {
    ($params:expr, $ty:ty, $id:expr) => {
        match serde_json::from_value::<$ty>($params) {
            Ok(v) => v,
            Err(e) => {
                return RpcResponse::err(
                    $id,
                    RPC_INVALID_PARAMS,
                    format!("invalid params: {e}"),
                )
            }
        }
    };
}

/// Run a mint method and convert its result into a JSON-RPC response.
macro_rules! run {
    ($id:expr, $expr:expr) => {{
        let id = $id;
        match $expr.await {
            Ok(v) => match serde_json::to_value(v) {
                Ok(json) => RpcResponse::ok(id, json),
                Err(e) => RpcResponse::err(id, RPC_INTERNAL_ERROR, e.to_string()),
            },
            Err(e) => {
                tracing::warn!("mint error: {e}");
                RpcResponse::err(id, RPC_INTERNAL_ERROR, e.to_string())
            }
        }
    }};
}

// ---------------------------------------------------------------------------
// Individual method handlers
// ---------------------------------------------------------------------------

async fn handle_get_keys(mint: &Arc<Mint>, id: serde_json::Value) -> RpcResponse {
    match serde_json::to_value(mint.pubkeys()) {
        Ok(json) => RpcResponse::ok(id, json),
        Err(e) => RpcResponse::err(id, RPC_INTERNAL_ERROR, e.to_string()),
    }
}

async fn handle_get_keysets(mint: &Arc<Mint>, id: serde_json::Value) -> RpcResponse {
    match serde_json::to_value(mint.keysets()) {
        Ok(json) => RpcResponse::ok(id, json),
        Err(e) => RpcResponse::err(id, RPC_INTERNAL_ERROR, e.to_string()),
    }
}

async fn handle_get_key_by_id(
    mint: &Arc<Mint>,
    params: serde_json::Value,
    id: serde_json::Value,
) -> RpcResponse {
    /// Parameters for `cashu_getKeyById`.
    ///
    /// The client (IrohAsync transport) sends `{ "id": "..." }` matching the
    /// URL path segment, so we accept either field name for compatibility.
    #[derive(serde::Deserialize)]
    struct Params {
        /// Keyset ID — sent as `"id"` by the IrohAsync URL-to-method mapper.
        #[serde(alias = "keyset_id")]
        id: Id,
    }

    let p = parse_params!(params, Params, id);

    match mint.keyset_pubkeys(&p.id) {
        Ok(keys) => match serde_json::to_value(keys) {
            Ok(json) => RpcResponse::ok(id, json),
            Err(e) => RpcResponse::err(id, RPC_INTERNAL_ERROR, e.to_string()),
        },
        Err(e) => RpcResponse::err(id, RPC_INTERNAL_ERROR, e.to_string()),
    }
}

async fn handle_post_swap(
    mint: &Arc<Mint>,
    params: serde_json::Value,
    id: serde_json::Value,
) -> RpcResponse {
    let req = parse_params!(params, SwapRequest, id);
    run!(id, mint.process_swap_request(req))
}

/// `cashu_getMintQuote` — **check status** of an existing mint quote.
///
/// This is triggered by `GET /v1/mint/quote/bolt11/{id}` from the IrohAsync
/// URL mapper, which passes `{ "id": "<quote-id>", "payment_method": "bolt11" }`.
async fn handle_get_mint_quote(
    mint: &Arc<Mint>,
    params: serde_json::Value,
    id: serde_json::Value,
) -> RpcResponse {
    use cdk::mint::MintQuoteResponse;

    #[derive(serde::Deserialize)]
    struct Params {
        /// The quote ID to look up (sent as `"id"` by the URL mapper).
        id: QuoteId,
    }

    let p = parse_params!(params, Params, id);

    match mint.check_mint_quote(&p.id).await {
        Ok(resp) => {
            let json_val = match resp {
                MintQuoteResponse::Bolt11(r) => serde_json::to_value(r),
                MintQuoteResponse::Bolt12(r) => serde_json::to_value(r),
                MintQuoteResponse::Custom { response, .. } => serde_json::to_value(response),
            };
            match json_val {
                Ok(v) => RpcResponse::ok(id, v),
                Err(e) => RpcResponse::err(id, RPC_INTERNAL_ERROR, e.to_string()),
            }
        }
        Err(e) => RpcResponse::err(id, RPC_INTERNAL_ERROR, e.to_string()),
    }
}

/// `cashu_postMintQuote` — **create** a new mint quote.
///
/// This is triggered by `POST /v1/mint/quote/bolt11` from the IrohAsync
/// URL mapper, which passes the full `MintQuoteBolt11Request` (or custom)
/// as params.
async fn handle_post_mint_quote(
    mint: &Arc<Mint>,
    params: serde_json::Value,
    id: serde_json::Value,
) -> RpcResponse {
    use cdk::mint::{MintQuoteRequest, MintQuoteResponse};
    use cdk::nuts::{MintQuoteBolt11Request, MintQuoteCustomRequest};

    #[derive(serde::Deserialize)]
    struct Params {
        #[serde(default = "default_bolt11")]
        payment_method: String,
        #[serde(flatten)]
        rest: serde_json::Value,
    }

    fn default_bolt11() -> String {
        "bolt11".to_string()
    }

    let p = parse_params!(params, Params, id);
    let method = p.payment_method.clone();
    let rest = p.rest;

    let quote_request: MintQuoteRequest = match method.as_str() {
        "bolt11" => {
            let inner = match serde_json::from_value::<MintQuoteBolt11Request>(rest) {
                Ok(v) => v,
                Err(e) => {
                    return RpcResponse::err(id, RPC_INVALID_PARAMS, e.to_string());
                }
            };
            inner.into()
        }
        other => {
            let inner = match serde_json::from_value::<MintQuoteCustomRequest>(rest) {
                Ok(v) => v,
                Err(e) => {
                    return RpcResponse::err(id, RPC_INVALID_PARAMS, e.to_string());
                }
            };
            MintQuoteRequest::Custom {
                method: other.to_string(),
                request: inner,
            }
        }
    };

    match mint.get_mint_quote(quote_request).await {
        Ok(resp) => {
            let json_val = match resp {
                MintQuoteResponse::Bolt11(r) => serde_json::to_value(r),
                MintQuoteResponse::Bolt12(r) => serde_json::to_value(r),
                MintQuoteResponse::Custom { response, .. } => serde_json::to_value(response),
            };
            match json_val {
                Ok(v) => RpcResponse::ok(id, v),
                Err(e) => RpcResponse::err(id, RPC_INTERNAL_ERROR, e.to_string()),
            }
        }
        Err(e) => RpcResponse::err(id, RPC_INTERNAL_ERROR, e.to_string()),
    }
}

async fn handle_post_mint(
    mint: &Arc<Mint>,
    params: serde_json::Value,
    id: serde_json::Value,
) -> RpcResponse {
    let req = parse_params!(params, MintRequest<QuoteId>, id);
    run!(id, mint.process_mint_request(req))
}

/// `cashu_getMeltQuote` — **check status** of an existing melt quote.
///
/// Triggered by `GET /v1/melt/quote/bolt11/{id}` from the IrohAsync URL mapper,
/// which passes `{ "id": "<quote-id>", "payment_method": "bolt11" }`.
async fn handle_get_melt_quote(
    mint: &Arc<Mint>,
    params: serde_json::Value,
    id: serde_json::Value,
) -> RpcResponse {
    #[derive(serde::Deserialize)]
    struct Params {
        /// The melt quote ID (sent as `"id"` by the URL mapper).
        id: QuoteId,
    }

    let p = parse_params!(params, Params, id);
    run!(id, mint.check_melt_quote(&p.id))
}

/// `cashu_postMeltQuote` — **create** a new melt quote.
///
/// Triggered by `POST /v1/melt/quote/bolt11` from the IrohAsync URL mapper,
/// which passes the full `MeltQuoteBolt11Request` (or custom) as params.
async fn handle_post_melt_quote(
    mint: &Arc<Mint>,
    params: serde_json::Value,
    id: serde_json::Value,
) -> RpcResponse {
    use cdk::nuts::MeltQuoteCustomRequest;

    #[derive(serde::Deserialize)]
    struct Params {
        #[serde(default = "default_bolt11")]
        payment_method: String,
        #[serde(flatten)]
        rest: serde_json::Value,
    }

    fn default_bolt11() -> String {
        "bolt11".to_string()
    }

    let p = parse_params!(params, Params, id);
    let method = p.payment_method.clone();
    let rest = p.rest;

    let melt_request: MeltQuoteRequest = match method.as_str() {
        "bolt11" => {
            let inner = match serde_json::from_value::<MeltQuoteBolt11Request>(rest) {
                Ok(v) => v,
                Err(e) => return RpcResponse::err(id, RPC_INVALID_PARAMS, e.to_string()),
            };
            inner.into()
        }
        _ => {
            let inner = match serde_json::from_value::<MeltQuoteCustomRequest>(rest) {
                Ok(v) => v,
                Err(e) => return RpcResponse::err(id, RPC_INVALID_PARAMS, e.to_string()),
            };
            inner.into()
        }
    };

    run!(id, mint.get_melt_quote(melt_request))
}

async fn handle_post_melt(
    mint: &Arc<Mint>,
    params: serde_json::Value,
    id: serde_json::Value,
) -> RpcResponse {
    let req = parse_params!(params, MeltRequest<QuoteId>, id);
    run!(id, mint.melt(&req))
}

async fn handle_check_state(
    mint: &Arc<Mint>,
    params: serde_json::Value,
    id: serde_json::Value,
) -> RpcResponse {
    let req = parse_params!(params, CheckStateRequest, id);
    run!(id, mint.check_state(&req))
}

async fn handle_get_info(mint: &Arc<Mint>, id: serde_json::Value) -> RpcResponse {
    match mint.mint_info().await {
        Ok(info) => {
            let info = info.time(unix_time());
            match serde_json::to_value(info) {
                Ok(json) => RpcResponse::ok(id, json),
                Err(e) => RpcResponse::err(id, RPC_INTERNAL_ERROR, e.to_string()),
            }
        }
        Err(e) => RpcResponse::err(id, RPC_INTERNAL_ERROR, e.to_string()),
    }
}

async fn handle_restore(
    mint: &Arc<Mint>,
    params: serde_json::Value,
    id: serde_json::Value,
) -> RpcResponse {
    let req = parse_params!(params, RestoreRequest, id);
    run!(id, mint.restore(req))
}
