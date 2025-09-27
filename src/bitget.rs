// src/connectors/bitget.rs

use crate::orderbook::WsOrderBookData;
use crate::state::AppState;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
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
    data: Option<Vec<WsOrderBookData>>,
}
#[derive(Deserialize, Debug)]
struct WsMessageArg {
    #[serde(rename = "instId")]
    #[allow(dead_code)]
    inst_id: String,
}

// --- Обработчик для одной пары ("Worker") ---
async fn pair_worker(
    symbol: String,
    inst_type: String,
    app_state: AppState,
    orderbook_update_tx: mpsc::Sender<String>,
    mut rx: mpsc::Receiver<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let log_prefix = format!("[{}/{}]", inst_type, symbol);
    let mut is_synced = false;

    info!("{} Worker started.", log_prefix);

    while let Some(msg_text) = rx.recv().await {
        if let Ok(msg) = serde_json::from_str::<WsMessageWrapper>(&msg_text) {
            if let Some(data_vec) = msg.data {
                if let Some(data) = data_vec.into_iter().next() {
                    let mut pair_data = app_state.inner.market_data.entry(symbol.clone()).or_default();
                    let book = if inst_type == "SPOT" { &mut pair_data.spot_book } else { &mut pair_data.futures_book };

                    match msg.action.as_deref() {
                        Some("snapshot") => {
                            book.apply_snapshot(&data);
                            is_synced = true;
                            info!("{} Synced from snapshot.", log_prefix);
                            // --- ОТПРАВКА СИГНАЛА ---
                            if orderbook_update_tx.try_send(symbol.clone()).is_err() {
                                // Канал переполнен, ничего страшного
                            }
                        }
                        Some("update") if is_synced => {
                            // Сначала всегда применяем обновление
                            book.apply_update(&data);

                            // Затем, если checksum есть, валидируем стакан
                            if let Some(server_checksum) = data.checksum {
                                if let Err(e) = book.validate(server_checksum) {
                                    // Если валидация провалилась, логируем ошибку
                                    error!("{} {}", log_prefix, e);
                                    
                                    // Сбрасываем флаг, чтобы остановить обработку и ждать нового snapshot'a
                                    is_synced = false; 
                                }
                            }

                            // --- ОТПРАВКА СИГНАЛА ---
                            if orderbook_update_tx.try_send(symbol.clone()).is_err() {
                                // Канал переполнен, ничего страшного
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    Ok(())
}

// --- Основная структура коннектора ---
// Теперь она просто хранит общие параметры
pub struct BitgetConnector {
    inst_type: String,
    inst_ids: Vec<String>,
    app_state: AppState,
    channel: String,
    orderbook_update_tx: mpsc::Sender<String>,
}

impl BitgetConnector {
    pub fn new(
        inst_type: String,
        inst_ids: Vec<String>,
        app_state: AppState,
        channel: String,
        orderbook_update_tx: mpsc::Sender<String>,
    ) -> Self {
        Self { inst_type, inst_ids, app_state, channel, orderbook_update_tx }
    }

    /// Управляет запуском нескольких соединений.
    pub async fn run(&self) {
        info!("[{}] Main connector manager starting for {} pairs.", self.inst_type, self.inst_ids.len());

        // Устанавливаем, сколько пар будет на одно WebSocket соединение.
        const PAIRS_PER_CONNECTION: usize = 50;

        let mut tasks = tokio::task::JoinSet::new();

        // Оптимизация: клонируем общие данные один раз перед циклом
        let _app_state = self.app_state.clone();
        let inst_type = self.inst_type.clone();
        let _channel = self.channel.clone();

        for (i, chunk) in self.inst_ids.chunks(PAIRS_PER_CONNECTION).enumerate() {
            // Создаем отдельный "суб-коннектор" для каждого пакета пар
            let sub_connector = SubConnector {
                log_prefix: format!("[{}-{}]", self.inst_type, i + 1),
                inst_type: inst_type.clone(),
                inst_ids: chunk.to_vec(),
                app_state: self.app_state.clone(),
                channel: self.channel.clone(),
                orderbook_update_tx: self.orderbook_update_tx.clone(),
            };
            
            // Запускаем для него бесконечный цикл переподключений в отдельной задаче
            tasks.spawn(async move {
                sub_connector.run_connection_loop().await;
            });
        }
        
        // Ожидаем завершения всех менеджеров (в теории, никогда)
        while let Some(res) = tasks.join_next().await {
            error!("[{}] A connection manager task unexpectedly finished: {:?}", self.inst_type, res);
        }
    }
}

// --- НОВАЯ СТРУКТУРА ДЛЯ ОДНОГО СОЕДИНЕНИЯ ---
struct SubConnector {
    log_prefix: String,
    inst_type: String,
    inst_ids: Vec<String>,
    app_state: AppState,
    channel: String,
    orderbook_update_tx: mpsc::Sender<String>,
}

impl SubConnector {
    /// Управляет циклом переподключений для одного соединения.
    async fn run_connection_loop(&self) {
        loop {
            info!("{} Attempting to connect for {} pairs...", self.log_prefix, self.inst_ids.len());

            let result = self.connect_and_process().await;

            // Логируем ошибку, только если она есть
            if let Err(e) = result {
                error!("{} Connector error: {}. Reconnecting...", self.log_prefix, e);
            }
            tokio::time::sleep(Duration::from_secs(5)).await; // Задержка перед следующей попыткой
        }
    }

    /// Выполняет работу в рамках одного WebSocket соединения.
    /// Этот код - это ваш предыдущий `connect_and_process` почти без изменений.
    async fn connect_and_process(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (ws_stream, _) = connect_async(BITGET_WS_URL).await?;
        info!("{} WebSocket connection successful.", self.log_prefix);
        let (mut writer, mut reader) = ws_stream.split();

        // Запуск worker'ов для пар ЭТОГО соединения
        let worker_channels = Arc::new(DashMap::<String, mpsc::Sender<String>>::new());
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
        
        // Подписка только на пары ЭТОГО соединения
        let args: Vec<SubscribeArg> = self.inst_ids.iter().map(|id| SubscribeArg {
            inst_type: self.inst_type.clone(),
            channel: self.channel.clone(),
            inst_id: id.clone(),
        }).collect();
        let sub_msg = SubscribeMessage { op: "subscribe".to_string(), args };
        let payload = Message::Text(serde_json::to_string(&sub_msg)?);
        writer.send(payload).await?;
        info!("{} Subscription message sent.", self.log_prefix);

        // Основной цикл с Ping/Pong и Диспетчером
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
                            // Быстрый поиск без полного парсинга. Ищем `"instId":"SYMBOL"`.
                            if let Some(start) = text.find("\"instId\":\"") {
                                let start = start + 10; // Смещаемся за кавычки
                                if let Some(end) = text[start..].find('"') {
                                    let symbol = &text[start..start + end];
                                    if let Some(tx) = worker_channels.get(symbol) {
                                        // `try_send` остается
                                        match tx.try_send(text) {
                                            Ok(_) => {
                                                // Сообщение успешно и мгновенно отправлено
                                            },
                                            Err(mpsc::error::TrySendError::Full(_)) => {
                                                // Буфер worker'а полон, он не успевает.
                                                // Это нормально для активных пар, просто пропускаем сообщение.
                                                trace!("{} Worker for {} is lagging, dropping message.", self.log_prefix, symbol);
                                            },
                                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                                // Worker завершил работу, его канал закрыт. Удаляем.
                                                worker_channels.remove(symbol);
                                            }
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
            }
        }
    }
}
