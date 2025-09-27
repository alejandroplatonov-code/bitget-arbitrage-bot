// src/connectors/bitget.rs

use crate::orderbook::WsOrderBookData;
use dashmap::DashMap;
use rust_decimal::Decimal;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use tokio::{sync::mpsc, time::{interval, Duration}};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, trace};

const BITGET_WS_URL: &str = "wss://ws.bitget.com/v2/ws/public";

// --- Структуры для сериализации/десериализации ---
#[derive(Serialize, Clone, Debug)]
struct SubscribeArg {
    #[serde(rename = "instType")]
    inst_type: String,
    channel: String,
    #[serde(rename = "instId")]
    inst_id: String,
}
#[derive(Serialize, Debug)]
struct SubscribeMessage {
    op: String,
    args: Vec<SubscribeArg>,
}
#[derive(Deserialize, Debug)]
struct WsMessageWrapper {
    action: Option<String>,
    arg: WsMessageArg,
    data: Option<serde_json::Value>, // Используем Value для обработки разных типов данных (books или trades)
}
#[derive(Deserialize, Debug)]
struct WsMessageArg {
    #[serde(rename = "instId")]
    inst_id: String,
    channel: String, // Добавляем канал для различения books и trades
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct WsTradeData {
    price: String,
    // Нам нужна только цена, остальные поля (tradeId, size, side, ts) для этой задачи не используются.
}

// --- Обработчик для одной пары ("Worker") ---
async fn pair_worker(
    symbol: String,
    inst_type: String,
    app_state: std::sync::Arc<crate::state::AppStateInner>,
    orderbook_update_tx: mpsc::Sender<String>,
    mut rx: mpsc::Receiver<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let log_prefix = format!("[{}/{}]", inst_type, symbol);
    let mut is_synced = false;

    info!("{} Worker started.", log_prefix);

    while let Some(msg_text) = rx.recv().await {
        if let Ok(msg) = serde_json::from_str::<WsMessageWrapper>(&msg_text) {
            if let Some(data_value) = msg.data {
                match msg.arg.channel.as_str() {
                    "books" => {
                        if let Ok(data_vec) = serde_json::from_value::<Vec<WsOrderBookData>>(data_value) {
                            if let Some(data) = data_vec.into_iter().next() {
                                let mut pair_data = app_state.market_data.entry(symbol.clone()).or_default();
                                let book = if inst_type == "SPOT" { &mut pair_data.spot_book } else { &mut pair_data.futures_book };

                                match msg.action.as_deref() {
                                    Some("snapshot") => {
                                        book.apply_snapshot(&data);
                                        is_synced = true;
                                        info!("{} Synced from snapshot.", log_prefix);
                                        if orderbook_update_tx.try_send(symbol.clone()).is_err() {
                                            // Канал переполнен, ничего страшного
                                        }
                                    }
                                    Some("update") if is_synced => {
                                        book.apply_update(&data);
                                        if let Some(server_checksum) = data.checksum {
                                            if let Err(e) = book.validate(server_checksum) {
                                                error!("{} {}", log_prefix, e);
                                                is_synced = false; 
                                            }
                                        }
                                        if orderbook_update_tx.try_send(symbol.clone()).is_err() {
                                            // Канал переполнен, ничего страшного
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                    },
                    "trades" => {
                        if let Ok(data_vec) = serde_json::from_value::<Vec<WsTradeData>>(data_value) {
                            if let Some(trade_data) = data_vec.into_iter().next() {
                                if let Ok(price) = Decimal::from_str(&trade_data.price) {
                                    let mut pair_data = app_state.market_data.entry(symbol.clone()).or_default();
                                    if inst_type == "SPOT" {
                                        pair_data.spot_last_price = Some(price);
                                    } else {
                                        pair_data.futures_last_price = Some(price);
                                    }
                                }
                            }
                        }
                    },
                    _ => {
                        trace!("{} Received message for unknown channel: {}", log_prefix, msg.arg.channel);
                    },
                }
            }
        }
    }
    Ok(())
}

// --- Основная структура коннектора ---
pub struct BitgetConnector {
    inst_type: String,
    inst_ids: Vec<String>,
    app_state: std::sync::Arc<crate::state::AppStateInner>,
    channels: Vec<String>, // Поддерживает несколько каналов (books, trades)
    orderbook_update_tx: mpsc::Sender<String>,
}

impl BitgetConnector {
    pub fn new(
        inst_type: String,
        inst_ids: Vec<String>,        
        app_state: std::sync::Arc<crate::state::AppStateInner>,
        channels: Vec<String>,
        orderbook_update_tx: mpsc::Sender<String>,
    ) -> Self {
        Self { inst_type, inst_ids, app_state, channels, orderbook_update_tx }
    }

    pub async fn run(self, shutdown: std::sync::Arc<tokio::sync::Notify>) {
        info!("[{}] Main connector manager starting for {} pairs.", self.inst_type, self.inst_ids.len());
        const PAIRS_PER_CONNECTION: usize = 50;
        let mut tasks = tokio::task::JoinSet::new();

        for (i, chunk) in self.inst_ids.chunks(PAIRS_PER_CONNECTION).enumerate() {
            let sub_connector = SubConnector {
                log_prefix: format!("[{}-{}]", self.inst_type, i + 1),
                inst_type: self.inst_type.clone(),
                inst_ids: chunk.to_vec(),
                app_state: self.app_state.clone(),
                channels: self.channels.clone(),
                orderbook_update_tx: self.orderbook_update_tx.clone(),
            };
            
            let shutdown_for_task = shutdown.clone();
            tasks.spawn(async move {
                sub_connector.run_connection_loop(shutdown_for_task).await;
            });
        }
        
        while let Some(res) = tasks.join_next().await {
            error!("[{}] A connection manager task unexpectedly finished: {:?}", self.inst_type, res);
        }
    }
}

// --- Структура для одного соединения ---
struct SubConnector {
    log_prefix: String,
    inst_type: String,
    inst_ids: Vec<String>,
    app_state: std::sync::Arc<crate::state::AppStateInner>,
    channels: Vec<String>,
    orderbook_update_tx: mpsc::Sender<String>,
}

impl SubConnector {
    async fn run_connection_loop(&self, shutdown: std::sync::Arc<tokio::sync::Notify>) {
        loop {
            info!("{} Attempting to connect for {} pairs...", self.log_prefix, self.inst_ids.len());
            if let Err(e) = self.connect_and_process(shutdown.clone()).await {
                error!("{} Connector error: {}. Reconnecting...", self.log_prefix, e);
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    async fn connect_and_process(&self, shutdown: std::sync::Arc<tokio::sync::Notify>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (ws_stream, _) = connect_async(BITGET_WS_URL).await?;
        info!("{} WebSocket connection successful.", self.log_prefix);
        let (mut writer, mut reader) = ws_stream.split();

        let worker_channels = std::sync::Arc::new(DashMap::<String, mpsc::Sender<String>>::new());
        for symbol in &self.inst_ids {
            let (worker_tx, worker_rx) = mpsc::channel(128);
            worker_channels.insert(symbol.clone(), worker_tx);
            tokio::spawn(pair_worker(
                symbol.clone(),
                self.inst_type.clone(),
                self.app_state.clone(),
                self.orderbook_update_tx.clone(),
                worker_rx,
            ));
        }
        
        // --- ИЗМЕНЕНИЕ: Подписка на несколько каналов ---
        // 1. Подписка на BOOKS
        let books_args: Vec<SubscribeArg> = self.inst_ids.iter().map(|id| SubscribeArg {
            inst_type: self.inst_type.clone(),
            channel: "books".to_string(),
            inst_id: id.clone(),
        }).collect();
        let books_sub_msg = SubscribeMessage { op: "subscribe".to_string(), args: books_args };
        writer.send(Message::Text(serde_json::to_string(&books_sub_msg)?)).await?;
        info!("{} Subscription message sent for 'books'.", self.log_prefix);
        tokio::time::sleep(Duration::from_millis(250)).await;

        // 2. Подписка на TRADES
        let trades_args: Vec<SubscribeArg> = self.inst_ids.iter().map(|id| SubscribeArg {
            inst_type: self.inst_type.clone(),
            channel: "trades".to_string(),
            inst_id: id.clone(),
        }).collect();
        let trades_sub_msg = SubscribeMessage { op: "subscribe".to_string(), args: trades_args };
        writer.send(Message::Text(serde_json::to_string(&trades_sub_msg)?)).await?;
        info!("{} Subscription message sent for 'trades'.", self.log_prefix);
        // --- КОНЕЦ ИЗМЕНЕНИЯ ---

        let mut ping_interval = interval(Duration::from_secs(25));
        loop {
            tokio::select! {
                _ = ping_interval.tick() => {
                    if writer.send(Message::Text("ping".to_string())).await.is_err() {
                        return Err("Failed to send ping.".into());
                    }
                }
                msg_result = reader.next() => {
                    match msg_result {
                        Some(Ok(Message::Text(text))) => {
                            if text == "pong" { continue; }
                            if let Ok(wrapper) = serde_json::from_str::<WsMessageWrapper>(&text) {
                                if let Some(tx) = worker_channels.get(&wrapper.arg.inst_id) {
                                    match tx.try_send(text) {
                                        Ok(_) => {},
                                        Err(mpsc::error::TrySendError::Full(_)) => {
                                            trace!("{} Worker for {} is lagging, dropping message.", self.log_prefix, wrapper.arg.inst_id);
                                        },
                                        Err(mpsc::error::TrySendError::Closed(_)) => {
                                            worker_channels.remove(&wrapper.arg.inst_id);
                                        }
                                    }
                                }
                            }
                        },
                        Some(Ok(_)) => {},
                        Some(Err(e)) => return Err(e.into()),
                        None => return Err("WebSocket stream closed.".into()),
                    }
                }
                _ = shutdown.notified() => {
                    info!("{} Shutdown signal received. Closing WebSocket connection.", self.log_prefix);
                    return Ok(());
                }
            }
        }
    }
}
