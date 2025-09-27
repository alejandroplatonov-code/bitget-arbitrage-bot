// src/private_connector.rs

use crate::api_client::ApiClient;
use crate::order_watcher::{OrderContext, OrderFilledEvent, OrderType};
use crate::state::AppState;
use crate::utils::send_cancellable;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Notify};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

const BITGET_PRIVATE_WS_URL: &str = "wss://ws.bitget.com/v2/ws/private";

// --- WebSocket Message Structures ---

#[derive(Serialize, Debug)]
struct WsAuthArg<'a> {
    #[serde(rename = "apiKey")]
    api_key: &'a str,
    passphrase: &'a str,
    timestamp: String,
    sign: String,
}

#[derive(Serialize, Debug)]
struct WsOp<'a, T> {
    op: &'a str,
    args: Vec<T>,
}

#[derive(Serialize, Debug)]
struct WsSubscribeArg {
    #[serde(rename = "instType")]
    inst_type: String,
    channel: String,
    #[serde(rename = "instId")]
    inst_id: String,
}

#[derive(Deserialize, Debug)]
struct WsResponse {
    event: Option<String>,
    code: Option<String>,
    msg: Option<String>,
    action: Option<String>,
    arg: Option<serde_json::Value>,
    data: Option<serde_json::Value>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct WsPrivateOrderData {
    inst_type: String,
    inst_id: String,
    order_id: String,
    cl_ord_id: String,
    state: String,
    avg_fill_px: String,
    acc_fill_sz: String,
    fill_total_amount: Option<String>,
}

/// Runs the private WebSocket connector for real-time order updates.
pub async fn run_private_ws_connector(
    api_client: Arc<ApiClient>,
    app_state: Arc<AppState>,
    order_filled_tx: mpsc::Sender<OrderFilledEvent>,
    shutdown: Arc<Notify>,
) {
    info!("[PrivateWS] Service started.");
    loop {
        tokio::select! {
            biased;
            _ = shutdown.notified() => {
                info!("[PrivateWS] Shutdown signal received. Terminating.");
                return;
            },
            result = connect_and_listen(api_client.clone(), app_state.clone(), order_filled_tx.clone(), shutdown.clone()) => {
                if let Err(e) = result {
                    error!("[PrivateWS] Connection error: {}. Reconnecting in 5s...", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                } else {
                    // Graceful shutdown
                    return;
                }
            }
        }
    }
}

async fn connect_and_listen(
    api_client: Arc<ApiClient>,
    app_state: Arc<AppState>,
    order_filled_tx: mpsc::Sender<OrderFilledEvent>,
    shutdown: Arc<Notify>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 1. Connect
    let (ws_stream, _) = connect_async(BITGET_PRIVATE_WS_URL).await?;
    info!("[PrivateWS] WebSocket connection successful.");
    let (mut writer, mut reader) = ws_stream.split();

    // 2. Authenticate
    let (api_key, passphrase) = api_client.get_ws_credentials();
    let (timestamp, sign) = api_client.get_ws_auth_args();
    let auth_msg = WsOp {
        op: "auth",
        args: vec![WsAuthArg { api_key, passphrase, timestamp, sign }],
    };
    writer.send(Message::Text(serde_json::to_string(&auth_msg)?)).await?;
    info!("[PrivateWS] Authentication message sent.");

    // 3. Subscribe
    let sub_args = vec![
        WsSubscribeArg { inst_type: "SPOT".to_string(), channel: "orders".to_string(), inst_id: "default".to_string() },
        WsSubscribeArg { inst_type: "USDT-FUTURES".to_string(), channel: "orders".to_string(), inst_id: "default".to_string() },
    ];
    let sub_msg = WsOp { op: "subscribe", args: sub_args };
    writer.send(Message::Text(serde_json::to_string(&sub_msg)?)).await?;
    info!("[PrivateWS] Subscription message sent for 'orders' channel.");

    // 4. Main listen loop
    let mut ping_interval = tokio::time::interval(Duration::from_secs(25));

    loop {
        tokio::select! {
            _ = ping_interval.tick() => {
                if writer.send(Message::Text("ping".to_string())).await.is_err() {
                    return Err("Failed to send ping.".into());
                }
            },
            Some(msg_result) = reader.next() => {
                match msg_result {
                    Ok(Message::Text(text)) => {
                        if text == "pong" { continue; }
                        if let Ok(response) = serde_json::from_str::<WsResponse>(&text) {
                            if response.event == Some("login".to_string()) {
                                info!("[PrivateWS] Authentication successful.");
                                continue;
                            }
                            if let Some(data_val) = response.data {
                                if let Ok(orders) = serde_json::from_value::<Vec<WsPrivateOrderData>>(data_val) {
                                    for order_update in orders {
                                        if order_update.state == "filled" {
                                            info!("[PrivateWS] Received FILLED event for order {}", order_update.order_id);

                                            // IMPORTANT: Get context from AppState
                                            if let Some((_, context)) = app_state.inner.order_contexts.remove(&order_update.cl_ord_id) {
                                                let order_type = if order_update.inst_type == "SPOT" { OrderType::Spot } else { OrderType::Futures };

                                                let event = OrderFilledEvent {
                                                    symbol: order_update.inst_id,
                                                    order_id: order_update.order_id,
                                                    order_type,
                                                    avg_price: order_update.avg_fill_px,
                                                    base_volume: order_update.acc_fill_sz,
                                                    quote_volume: order_update.fill_total_amount,
                                                    client_oid: order_update.cl_ord_id,
                                                    context,
                                                };

                                                if !send_cancellable(&order_filled_tx, event, &shutdown).await {
                                                    warn!("[PrivateWS] Failed to send event to PositionManager (channel closed or shutdown).");
                                                }
                                            } else {
                                                warn!("[PrivateWS] Received filled event for untracked client_oid: {}", order_update.cl_ord_id);
                                            }
                                        }
                                    }
                                }
                            }
                        } else {
                            warn!("[PrivateWS] Failed to parse message: {}", text);
                        }
                    },
                    Ok(_) => {}, // Ignore other message types
                    Err(e) => return Err(e.into()),
                    None => return Err("WebSocket stream closed.".into()),
                }
            },
            _ = shutdown.notified() => {
                info!("[PrivateWS] Shutdown signal received. Closing WebSocket connection.");
                writer.close().await?;
                return Ok(());
            }
        }
    }
}