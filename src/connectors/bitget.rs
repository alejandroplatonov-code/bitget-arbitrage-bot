// src/connectors/bitget.rs

use crate::orderbook::WsOrderBookData;
use std::sync::atomic::{AtomicUsize, Ordering};
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::{sync::mpsc, time::{interval, Duration}};
use tokio::task::JoinSet;
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
    data: Option<serde_json::Value>, // Use Value to handle different data types (books or trades)
}
#[derive(Deserialize, Debug)]
struct WsMessageArg {
    #[serde(rename = "instId")]
    inst_id: String,
    channel: String, // Add channel to distinguish between books and trades
}

// --- Обработчик для одной пары ("Worker") ---
async fn pair_worker(
    symbol: String,
    inst_type: String,
    app_state: std::sync::Arc<crate::state::AppStateInner>,
    orderbook_update_tx: mpsc::Sender<String>,
    mut rx: mpsc::Receiver<String>,
    synced_counter: std::sync::Arc<AtomicUsize>,
    shutdown: std::sync::Arc<tokio::sync::Notify>, // <-- Добавляем shutdown
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let log_prefix = format!("[{}/{}]", inst_type, symbol);
    let mut is_synced = false;

    loop {
        tokio::select! {
            biased;
            _ = shutdown.notified() => {
                info!("{} Worker received shutdown signal. Exiting.", log_prefix);
                break;
            },
            Some(msg_text) = rx.recv() => {
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
                                        // Увеличиваем счетчик вместо логирования
                                        synced_counter.fetch_add(1, Ordering::Relaxed);
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
                            _ => {
                                trace!("{} Received message for unknown channel: {}", log_prefix, msg.arg.channel);
                            },
                        }
                    }
                }
            },
            else => {
                // Канал закрыт, выходим
                break;
            }
        }
    }
    info!("{} Worker finished.", log_prefix);
    Ok(())
}

// --- Основная структура коннектора ---
// Теперь она просто хранит общие параметры
pub struct BitgetConnector {
    inst_type: String,
    inst_ids: Vec<String>,
    app_state: std::sync::Arc<crate::state::AppStateInner>,
    channels: Vec<String>, // Changed to Vec<String> to support multiple channels (books, trades)
    orderbook_update_tx: mpsc::Sender<String>,
}

impl BitgetConnector {
    pub fn new(
        inst_type: String,
        inst_ids: Vec<String>,        
        app_state: std::sync::Arc<crate::state::AppStateInner>,
        channels: Vec<String>, // Changed to Vec<String>
        orderbook_update_tx: mpsc::Sender<String>,
    ) -> Self {
        Self { inst_type, inst_ids, app_state, channels, orderbook_update_tx }
    }

    /// Управляет запуском нескольких соединений.
    pub async fn run(self, shutdown: std::sync::Arc<tokio::sync::Notify>) { // self is consumed here
        info!("[{}] Main connector manager starting for {} pairs.", self.inst_type, self.inst_ids.len());

        // Устанавливаем, сколько пар будет на одно WebSocket соединение.
        const PAIRS_PER_CONNECTION: usize = 50;

        let mut tasks = tokio::task::JoinSet::new();

        // Оптимизация: клонируем общие данные один раз перед циклом
        let _app_state = self.app_state.clone();
        let inst_type = self.inst_type.clone();
        let _channels = self.channels.clone(); // Use _channels

        for (i, chunk) in self.inst_ids.chunks(PAIRS_PER_CONNECTION).enumerate() {
            // Создаем отдельный "суб-коннектор" для каждого пакета пар
            let sub_connector = SubConnector {
                log_prefix: format!("[{}-{}]", self.inst_type, i + 1),
                inst_type: inst_type.clone(),
                inst_ids: chunk.to_vec(),
                app_state: self.app_state.clone(),
                channels: self.channels.clone(), // Pass all channels
                orderbook_update_tx: self.orderbook_update_tx.clone(),
            };
            
            // Запускаем для него бесконечный цикл переподключений в отдельной задаче
            let shutdown_for_task = shutdown.clone();
            tasks.spawn(async move {
                sub_connector.run_connection_loop(shutdown_for_task).await;
            });
        }
        
        // Этот цикл следит за здоровьем под-коннекторов и реагирует на сигнал shutdown.
        loop {
            tokio::select! {
                // Ветка 1: Одна из дочерних задач неожиданно завершилась
                Some(res) = tasks.join_next() => {
                    // Логируем ошибку, но не выходим из цикла.
                    // Это позволяет остальным соединениям продолжать работать.
                    // В реальной системе здесь можно было бы реализовать перезапуск упавшей задачи.
                    error!("[{}] A sub-connector task finished unexpectedly: {:?}. Other connections remain active.", self.inst_type, res);
                },

                // Ветка 2: Получен глобальный сигнал о завершении работы
                _ = shutdown.notified() => {
                    info!("[{}] Main connector manager received shutdown. Aborting all sub-connectors.", self.inst_type);
                    // Принудительно отменяем все запущенные задачи-субконнекторы.
                    // Каждая из них поймает этот abort через свой `shutdown.notified()`.
                    tasks.abort_all();
                    // Выходим из цикла, чтобы завершить главную задачу менеджера.
                    break; 
                }
            }
        }
        info!("[{}] Main connector manager has shut down.", self.inst_type);
    }
}

// --- НОВАЯ СТРУКТУРА ДЛЯ ОДНОГО СОЕДИНЕНИЯ ---
struct SubConnector {
    log_prefix: String,
    inst_type: String,
    inst_ids: Vec<String>,
    app_state: std::sync::Arc<crate::state::AppStateInner>,
    channels: Vec<String>, // Changed to Vec<String>
    orderbook_update_tx: mpsc::Sender<String>,
}

impl SubConnector {
    /// Управляет циклом переподключений для одного соединения.
    async fn run_connection_loop(&self, shutdown: std::sync::Arc<tokio::sync::Notify>) {
        loop {
            info!("{} Attempting to connect for {} pairs...", self.log_prefix, self.inst_ids.len());
    
            // Используем select! для прерывания процесса, если сигнал придет во время работы
            tokio::select! {
                // `biased` гарантирует, что сигнал о завершении будет проверен первым
                biased;
    
                // Ветка 1: Сигнал о завершении работы получен
                _ = shutdown.notified() => {
                    info!("{} Connection loop received shutdown signal. Terminating.", self.log_prefix);
                    return; // Немедленно выходим из функции и завершаем задачу
                },
    
                // Ветка 2: Основная работа - попытка подключения и обработки
                result = self.connect_and_process(shutdown.clone()) => {
                    // Эта ветка сработает, когда connect_and_process завершится.
                    if let Err(e) = result {
                        error!("{} Connection error: {}. Will retry after delay.", self.log_prefix, e);
                    } else {
                        // Если connect_and_process вернул Ok(()), это значит,
                        // что он завершился штатно по сигналу shutdown.
                        // В этом случае мы тоже должны завершить внешний цикл.
                        info!("{} Inner connection loop finished gracefully. Terminating.", self.log_prefix);
                        return;
                    }
                }
            }
    
            // Если мы дошли сюда, значит, была ошибка. Ждем перед переподключением,
            // но также слушаем сигнал shutdown во время ожидания.
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(5)) => {
                    // Пауза завершена, основной loop продолжится для новой попытки.
                },
                _ = shutdown.notified() => {
                    info!("{} Shutdown signal received during reconnect delay. Terminating.", self.log_prefix);
                    return; // Выходим из функции, не дожидаясь окончания паузы
                }
            }
        }
    }

    /// Выполняет работу в рамках одного WebSocket соединения.
    /// Этот код - это ваш предыдущий `connect_and_process` почти без изменений.
    async fn connect_and_process(&self, shutdown: std::sync::Arc<tokio::sync::Notify>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (ws_stream, _) = connect_async(BITGET_WS_URL).await?;
        info!("{} WebSocket connection successful.", self.log_prefix);
        let (mut writer, mut reader) = ws_stream.split();

        // --- НОВЫЙ БЛОК: Групповое логирование синхронизации ---
        let synced_counter = std::sync::Arc::new(AtomicUsize::new(0));
        let total_pairs = self.inst_ids.len();
        let log_prefix_clone = self.log_prefix.clone();
        let synced_counter_clone = synced_counter.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                let count = synced_counter_clone.load(Ordering::Relaxed);
                if count >= total_pairs {
                    info!("{} All {} pairs synced from snapshots.", log_prefix_clone, total_pairs);
                    break; // Завершаем задачу после вывода лога
                }
            }
        });
    
        // --- ИЗМЕНЕНИЕ 1: Используем JoinSet для управления воркерами ---
        let mut worker_tasks = JoinSet::new();
        let worker_channels = std::sync::Arc::new(DashMap::<String, mpsc::Sender<String>>::new());
    

        for symbol in &self.inst_ids {
            let (worker_tx, worker_rx) = mpsc::channel(128);
            worker_channels.insert(symbol.clone(), worker_tx);
    
            // Запускаем воркеры внутри JoinSet, чтобы ими можно было управлять
            worker_tasks.spawn(pair_worker(
                symbol.clone(),
                self.inst_type.clone(),
                self.app_state.clone(),
                self.orderbook_update_tx.clone(),
                worker_rx,
                synced_counter.clone(), // Передаем счетчик в воркер
                shutdown.clone(), // Передаем shutdown в воркер
            ));
        }
        info!("{} Started {} pair workers.", self.log_prefix, self.inst_ids.len());
        
        // Код подписки остается без изменений
        let mut args: Vec<SubscribeArg> = Vec::new();
        for channel in &self.channels {
            for id in &self.inst_ids {
                args.push(SubscribeArg { inst_type: self.inst_type.clone(), channel: channel.clone(), inst_id: id.clone() });
            }
        }
        let sub_msg = SubscribeMessage { op: "subscribe".to_string(), args };
        let payload = Message::Text(serde_json::to_string(&sub_msg)?);
        writer.send(payload).await?;
        info!("{} Subscription message sent.", self.log_prefix);
    
        let mut ping_interval = interval(Duration::from_secs(25));
        loop {
            tokio::select! {
                _ = ping_interval.tick() => {
                    if writer.send(Message::Text("ping".to_string())).await.is_err() {
                        // --- ИЗМЕНЕНИЕ 2: Отменяем воркеры перед выходом по ошибке ---
                        worker_tasks.abort_all();
                        return Err("Failed to send ping.".into());
                    }
                }
                msg_result = reader.next() => {
                    match msg_result {
                        Some(Ok(Message::Text(text))) => {
                            // ... (внутренняя логика обработки сообщений остается без изменений)
                            if text == "pong" { continue; }
                            if let Ok(wrapper) = serde_json::from_str::<WsMessageWrapper>(&text) {
                                if let Some(tx) = worker_channels.get(&wrapper.arg.inst_id) {
                                    if let Err(e) = tx.try_send(text) {
                                        // Обработка ошибок try_send
                                        if let mpsc::error::TrySendError::Closed(_) = e {
                                            worker_channels.remove(&wrapper.arg.inst_id);
                                        }
                                    }
                                }
                            }
                        },
                        Some(Ok(_)) => {},
                        Some(Err(e)) => {
                            // --- ИЗМЕНЕНИЕ 2: Отменяем воркеры перед выходом по ошибке ---
                            worker_tasks.abort_all();
                            return Err(e.into());
                        },
                        None => {
                            // --- ИЗМЕНЕНИЕ 2: Отменяем воркеры перед выходом по ошибке ---
                            worker_tasks.abort_all();
                            return Err("WebSocket stream closed.".into());
                        },
                    }
                }
                _ = shutdown.notified() => {
                    info!("{} Shutdown signal received. Aborting all workers and closing connection.", self.log_prefix);
                    // --- ИЗМЕНЕНИЕ 3 (КЛЮЧЕВОЕ): Принудительно завершаем все дочерние задачи ---
                    worker_tasks.abort_all();
                    return Ok(()); // Выходим из функции
                }
            }
        }
    }
}
