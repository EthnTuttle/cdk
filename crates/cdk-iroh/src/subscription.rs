//! NUT-17 long-lived subscription stream handler for the Cashu/Iroh server.
//!
//! When the wallet sends a `cashu_subscribe` JSON-RPC request on a bidi QUIC
//! stream, the [`handle_subscription_stream`] function takes over that stream
//! and keeps it open for the lifetime of the subscription session.
//!
//! ## Protocol on the stream
//!
//! ```text
//! wallet → server  :  cashu_subscribe  (the initial request, already read by handle_stream)
//! server → wallet  :  {"jsonrpc":"2.0","result":{"status":"OK"},"id":0}
//!
//! wallet → server  :  WsRequest  (subscribe / unsubscribe)
//! server → wallet  :  WsMessageOrResponse  (response or notification)
//! ```
//!
//! Both directions use the same 4-byte BE length-prefix + JSON framing used
//! by every other stream in this protocol.

use std::collections::HashMap;
use std::sync::Arc;

use cdk::mint::{Mint, QuoteId};
use cdk::subscription::SubId;
use cdk_common::ws::{
    WsErrorBody, WsMessageOrResponse, WsMethodRequest, WsRequest, notification_to_ws_message,
    NotificationInner,
};
use tokio::sync::mpsc;

use crate::error::Error;
use crate::wire::{RpcResponse, read_message, write_message};

/// Handle a long-lived NUT-17 subscription stream.
///
/// The caller is responsible for having already read the initial
/// `cashu_subscribe` request from `recv` and passing its `id` here so that
/// we can send the opening acknowledgement response.
///
/// After that the stream enters a `select!` loop that concurrently:
/// - reads incoming `WsRequest` frames from the wallet (subscribe / unsubscribe)
/// - forwards `WsNotification` frames to the wallet whenever the pubsub
///   manager delivers events for registered subscriptions
pub async fn handle_subscription_stream(
    mint: Arc<Mint>,
    mut send: iroh::endpoint::SendStream,
    mut recv: iroh::endpoint::RecvStream,
    initial_id: serde_json::Value,
) -> Result<(), Error> {
    // -----------------------------------------------------------------------
    // 1. Send the opening acknowledgement so the wallet knows the stream is
    //    ready for subscribe/unsubscribe traffic.
    // -----------------------------------------------------------------------
    let ack = RpcResponse::ok(initial_id, serde_json::json!({"status": "OK"}));
    let ack_val = serde_json::to_value(&ack).map_err(|e| Error::Json(e.to_string()))?;
    write_message(&mut send, &ack_val).await?;

    // -----------------------------------------------------------------------
    // 2. Per-subscription state: task handles and mpsc sender/receiver for
    //    forwarding events from the pubsub manager back to this loop.
    // -----------------------------------------------------------------------

    // `pubsub` is `Arc<PubSubManager>` — we keep it as an opaque Arc and only
    // call `.subscribe(params)` on it, which is publicly accessible via the
    // `Deref` impl on `PubSubManager` (deref target: `Pubsub<MintPubSubSpec>`).
    let pubsub = mint.pubsub_manager();

    // A single channel that all per-subscription tasks funnel events into.
    let (event_tx, mut event_rx) =
        mpsc::channel::<(Arc<SubId>, cdk::nuts::nut17::NotificationPayload<QuoteId>)>(256);

    // Map from SubId → JoinHandle for the forwarding task.
    let mut subscriptions: HashMap<Arc<SubId>, tokio::task::JoinHandle<()>> = HashMap::new();

    // -----------------------------------------------------------------------
    // 3. Main select! loop
    // -----------------------------------------------------------------------
    loop {
        tokio::select! {
            // ----------------------------------------------------------------
            // 3a. Incoming event from one of the subscription tasks → forward
            //     as a WsNotification to the wallet.
            // ----------------------------------------------------------------
            Some((sub_id, payload)) = event_rx.recv() => {
                // Ignore stale events for subscriptions that have since been
                // unsubscribed (mirrors the cdk-axum ws behaviour).
                if !subscriptions.contains_key(&sub_id) {
                    continue;
                }

                let notification = notification_to_ws_message(NotificationInner {
                    sub_id,
                    payload,
                });

                let val = match serde_json::to_value(&notification) {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::error!("failed to serialize notification: {e}");
                        continue;
                    }
                };

                if let Err(e) = write_message(&mut send, &val).await {
                    tracing::warn!("subscription stream write error: {e}");
                    break;
                }
            }

            // ----------------------------------------------------------------
            // 3b. Incoming frame from the wallet → parse as WsRequest and
            //     handle subscribe / unsubscribe.
            // ----------------------------------------------------------------
            read_result = read_message(&mut recv) => {
                let raw = match read_result {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::debug!("subscription stream recv closed: {e}");
                        break;
                    }
                };

                // Parse as NUT-17 WsRequest
                let ws_req: WsRequest = match serde_json::from_value(raw) {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::warn!("subscription stream: invalid WsRequest: {e}");
                        // Send a parse-error response and keep the stream open.
                        let err_body = WsErrorBody {
                            code: -32700,
                            message: format!("parse error: {e}"),
                        };
                        let resp: WsMessageOrResponse =
                            (0usize, Err::<cdk_common::ws::WsResponseResult, _>(err_body)).into();
                        if let Ok(v) = serde_json::to_value(&resp) {
                            let _ = write_message(&mut send, &v).await;
                        }
                        continue;
                    }
                };

                let req_id = ws_req.id;

                let result: Result<cdk_common::ws::WsResponseResult, WsErrorBody> =
                    match ws_req.method {
                        WsMethodRequest::Subscribe(params) => {
                            let sub_id = params.id.clone();

                            if subscriptions.contains_key(&sub_id) {
                                Err(WsErrorBody {
                                    code: -32602,
                                    message: "subscription ID already exists".to_string(),
                                })
                            } else {
                                match pubsub.subscribe(params) {
                                    Err(_) => Err(WsErrorBody {
                                        code: -32700,
                                        message: "parse error".to_string(),
                                    }),
                                    Ok(mut subscription) => {
                                        let tx = event_tx.clone();
                                        let sub_id_for_task = sub_id.clone();
                                        let handle = tokio::spawn(async move {
                                            while let Some(event) = subscription.recv().await {
                                                let payload = event.into_inner();
                                                let _ = tx.try_send((
                                                    sub_id_for_task.clone(),
                                                    payload,
                                                ));
                                            }
                                        });
                                        subscriptions.insert(sub_id.clone(), handle);
                                        Ok(cdk_common::ws::WsSubscribeResponse {
                                            status: "OK".to_string(),
                                            sub_id,
                                        }
                                        .into())
                                    }
                                }
                            }
                        }
                        WsMethodRequest::Unsubscribe(unsub) => {
                            if let Some(handle) = subscriptions.remove(&unsub.sub_id) {
                                handle.abort();
                                Ok(cdk_common::ws::WsUnsubscribeResponse {
                                    status: "OK".to_string(),
                                    sub_id: unsub.sub_id,
                                }
                                .into())
                            } else {
                                Err(WsErrorBody {
                                    code: -32602,
                                    message: "unknown subscription ID".to_string(),
                                })
                            }
                        }
                    };

                let response: WsMessageOrResponse = (req_id, result).into();
                let val = match serde_json::to_value(&response) {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::error!("failed to serialize ws response: {e}");
                        break;
                    }
                };
                if let Err(e) = write_message(&mut send, &val).await {
                    tracing::warn!("subscription stream write error: {e}");
                    break;
                }
            }
        }
    }

    // Abort all subscription forwarding tasks before returning so they don't
    // outlive the stream and accumulate in the runtime.
    for (_, handle) in subscriptions.drain() {
        handle.abort();
    }

    Ok(())
}
