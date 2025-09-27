// src/bin/bot.rs

use rust_template_for_testing::{
    algorithm::run_trading_algorithm,
    api_client::ApiClient,
    config::Config,
    config::load_token_list,
    connectors::bitget::BitgetConnector,
    order_watcher::{run_order_watcher, OrderFilledEvent},
    position_manager::run_position_manager,
    error::AppError,
    state::AppState, // SymbolRules is fetched but only used inside algorithm.rs
    types::{TradingStatus, WsCommand},
};
use std::{time::Duration, str::FromStr};
use futures_util::future::FutureExt;
use futures_util::StreamExt;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info, trace, warn, Level};
use tokio::sync::Notify;
use tracing_subscriber::FmtSubscriber;
use serde::Deserialize;
#[derive(Deserialize, Debug)]
struct SpotSymbolInfo {
    symbol: String,
    #[serde(rename = "quantityScale")]
    quantity_scale: Option<String>, // Make optional to handle missing fields from API
}

#[derive(Deserialize, Debug)]
struct FuturesSymbolInfo {
    symbol: String,
    #[serde(rename = "sizeMultiplier")]
    size_multiplier: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // --- 1. Инициализация Логирования ---
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    info!("[Bot] Trading Bot starting...");

    // --- 2. Загрузка конфигурации ---
    let config = match Config::load() {
        Ok(cfg) => Arc::new(cfg),
        Err(e) => {
            error!("[Bot] FATAL: Failed to load config.toml: {}", e);
            return Err(e.into());
        }
    };
    info!("[Bot] Configuration loaded successfully.");

    // --- 3. Инициализация состояния и каналов ---
    let app_state = Arc::new(AppState::new());
    let (orderbook_update_tx, orderbook_update_rx) = mpsc::channel::<String>(1024);
    let (order_watch_tx, order_watch_rx) = mpsc::channel(256);
    let (order_filled_tx, order_filled_rx) = mpsc::channel(256);
    let (bot_command_tx, bot_command_rx) = mpsc::channel(32);
    let redis_client = redis::Client::open("redis://127.0.0.1/")?; // Client is clonable
    let _redis_publisher = redis_client.get_multiplexed_tokio_connection().await?; // Keep for potential future use, but not for PM/Watcher
    info!("[Bot] Redis client initialized.");

    // --- 4. Инициализация API клиента ---
    let api_client = Arc::new(ApiClient::new(
        config.api_keys.api_key.clone(),
        config.api_keys.api_secret.clone(),
        config.api_keys.api_passphrase.clone(),
    ));

    // --- 5. Запуск коннекторов для получения данных ---
    info!("[Bot] Fetching symbol trading rules...");
    if let Err(e) = fetch_and_store_symbol_rules(app_state.clone()).await {
        error!("[Bot] FATAL: Failed to fetch symbol rules: {}", e);
        return Err(e.into());
    }
    info!("[Bot] Symbol trading rules loaded successfully.");


    // --- 5. Запуск коннекторов для получения данных ---
    let trading_pairs = match load_token_list() {
        Ok(pairs) => pairs,
        Err(e) => {
            error!("[Bot] FATAL: Failed to load token list from tokens.txt: {}", e);
            return Err(e.into());
        }
    };
    info!("[Bot] Starting data connectors for {} pairs.", trading_pairs.len());

    let spot_connector = BitgetConnector::new(
        "SPOT".to_string(),
        trading_pairs.clone(),
        app_state.inner.clone(),
        vec!["books".to_string(), "trades".to_string()],
        orderbook_update_tx.clone(),
    );

    let futures_connector = BitgetConnector::new(
        "USDT-FUTURES".to_string(),
        trading_pairs,
        app_state.inner.clone(),
        vec!["books".to_string(), "trades".to_string()],
        orderbook_update_tx,
    );
    // --- НОВЫЙ БЛОК: Механизм Graceful Shutdown ---
    let shutdown_notify = Arc::new(Notify::new());
    let shutdown_signal_task = {
        let notify = shutdown_notify.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.expect("Failed to install Ctrl+C handler");
            info!("[Bot] CTRL-C received. Notifying all tasks to shut down...");
            notify.notify_waiters();
        })
    };
    // --- КОНЕЦ БЛОКА --- // spot_connector is moved here
    let spot_handle = tokio::spawn(spot_connector.run(shutdown_notify.clone()));
    info!("[Bot] Spot Connector spawned.");

    let futures_handle = tokio::spawn(futures_connector.run(shutdown_notify.clone()));
    info!("[Bot] Futures Connector spawned.");

    // --- 6. Запуск наблюдателя за ордерами ---
    let order_watcher_handle = tokio::spawn(run_order_watcher(
        order_watch_rx,
        api_client.clone(),
        order_filled_tx.clone(), // <-- ОТПРАВЛЯЕТ В MPSC
        app_state.clone(),
        shutdown_notify.clone()
    ));
    info!("[Bot] Order Watcher started.");

    // --- 7. Запуск менеджера позиций ---
    let position_manager_handle = tokio::spawn(run_position_manager(
        app_state.clone(), 
        order_filled_rx, // <-- ПРИНИМАЕТ ИЗ MPSC
        shutdown_notify.clone()
    ));
    info!("[Bot] Position Manager started.");

    // --- 9. ЗАПУСК СЛУШАТЕЛЯ КОМАНД ---
    let cmd_listener_handle = tokio::spawn(command_listener_task( // Now receives from mpsc
        app_state.clone(),
        bot_command_rx, // <-- ПРИНИМАЕТ ИЗ MPSC
        shutdown_notify.clone()
    ));
    info!("[Bot] Command Listener started.");

    // --- НОВЫЙ БЛОК: Запуск единого диспетчера событий Redis ---
    let redis_dispatcher_handle = tokio::spawn(redis_event_dispatcher_task(
        redis_client, // Pass the client itself
        order_filled_tx, // Он будет писать в этот канал
        bot_command_tx,
        shutdown_notify.clone(),
    ));
    info!("[Bot] Redis Event Dispatcher started.");

    // --- 8. Запуск основного алгоритма ---
    let algorithm_handle = tokio::spawn(run_trading_algorithm(
        app_state.clone(),
        config.as_ref().clone(),
        api_client.clone(),
        order_watch_tx,
        orderbook_update_rx, shutdown_notify.clone()
    ));
    info!("[Bot] Trading Algorithm started.");

    // --- 10. Ожидание завершения любой из задач ---
    tokio::select! {
        _ = shutdown_signal_task => { /* Ctrl+C был нажат, просто ждем завершения других задач */ },
        _ = spot_handle => warn!("[Bot] Spot connector task finished unexpectedly."),
        _ = futures_handle => warn!("[Bot] Futures connector task finished unexpectedly."),
        _ = algorithm_handle => warn!("[Bot] Trading algorithm task finished unexpectedly."),
        _ = order_watcher_handle => warn!("[Bot] Order watcher task finished unexpectedly."),
        _ = position_manager_handle => warn!("[Bot] Position manager task finished unexpectedly."),
        _ = cmd_listener_handle => warn!("[Bot] Command listener task finished unexpectedly."),
        _ = redis_dispatcher_handle => warn!("[Bot] Redis event dispatcher task finished unexpectedly."),
    }

    info!("[Bot] Shutdown signal received. Waiting for tasks to complete...");
    // Даем задачам немного времени на завершение
    tokio::time::sleep(Duration::from_secs(1)).await;
    info!("[Bot] Application shut down gracefully.");
    Ok(())
}

/// Fetches spot and futures trading rules from Bitget and stores them in AppState.
async fn fetch_and_store_symbol_rules(app_state: Arc<AppState>) -> Result<(), AppError> {
    // --- Fetch Spot Rules ---
    let spot_rules_response = reqwest::get("https://api.bitget.com/api/v2/spot/public/symbols").await?;
    let spot_rules_data: serde_json::Value = spot_rules_response.json().await?;
    if spot_rules_data["code"].as_str() != Some("00000") {
        return Err(AppError::LogicError(format!("Failed to fetch spot rules: {}", spot_rules_data["msg"])));
    }
    let spot_symbols: Vec<SpotSymbolInfo> = serde_json::from_value(spot_rules_data["data"].clone())?;

    for spot_info in spot_symbols {
        let mut rules = app_state.inner.symbol_rules.entry(spot_info.symbol).or_default();
        // Если поле есть и парсится, записываем Some(scale), иначе остается None
        rules.spot_quantity_scale = spot_info.quantity_scale.and_then(|s| s.parse().ok());
        if rules.spot_quantity_scale.is_none() {
            // Log if a trading pair is missing rules, it will be caught by verification later.
            warn!("[RulesLoader] Spot symbol {} is missing 'quantityScale'.", rules.key());
        }
    }

    // --- Fetch Futures Rules ---
    let futures_rules_response = reqwest::get("https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES").await?;
    let futures_rules_data: serde_json::Value = futures_rules_response.json().await?;
    if futures_rules_data["code"].as_str() != Some("00000") {
        return Err(AppError::LogicError(format!("Failed to fetch futures rules: {}", futures_rules_data["msg"])));
    }
    let futures_symbols: Vec<FuturesSymbolInfo> = serde_json::from_value(futures_rules_data["data"].clone())?;

    for fut_info in futures_symbols {
        let mut rules = app_state.inner.symbol_rules.entry(fut_info.symbol).or_default();
        // Парсим множитель, и если он валидный, вычисляем и сохраняем его scale.
        if let Ok(multiplier) = rust_decimal::Decimal::from_str(&fut_info.size_multiplier) {
            // scale() возвращает u32, что соответствует нашему новому типу поля
            rules.futures_quantity_scale = Some(multiplier.scale());
        }
    }

    // --- Verification (Optional but recommended) ---
    let trading_pairs = load_token_list()?;
    let mut missing_rules_count = 0;
    for pair in trading_pairs {
        if let Some(rules) = app_state.inner.symbol_rules.get(&pair) {
            if rules.spot_quantity_scale.is_none() || rules.futures_quantity_scale.is_none() {
                warn!("[RulesLoader] Incomplete rules for {}: {:?}", pair, rules.value());
                missing_rules_count += 1;
            }
        } else {
            warn!("[RulesLoader] No rules found for trading pair: {}", pair);
            missing_rules_count += 1;
        }
    }

    if missing_rules_count > 0 {
        error!("[RulesLoader] {} pairs have missing or incomplete trading rules. This may cause order placement failures.", missing_rules_count);
    } else {
        info!("[RulesLoader] All trading pairs have their rules loaded.");
    }

    Ok(())
}


/// A central hub for all incoming Redis Pub/Sub events.
/// It listens on multiple channels and forwards messages to the appropriate handlers via mpsc channels.
async fn redis_event_dispatcher_task(
    redis_client: redis::Client, // <-- Принимаем клиент
    order_filled_tx: mpsc::Sender<OrderFilledEvent>,
    bot_command_tx: mpsc::Sender<WsCommand>,
    shutdown: Arc<Notify>,
) {
    info!("[RedisDispatcher] Task starting. Subscribing to 'order_filled_events' and 'bot_commands'...");

    // Создаем соединение внутри задачи, чтобы оно было изолированным
    let mut pubsub = match redis_client.get_tokio_connection().await {
        Ok(conn) => conn.into_pubsub(),
        Err(e) => { error!("[RedisDispatcher] FATAL: Could not get Redis connection: {}", e); return; }
    };

    if let Err(e) = pubsub.subscribe(&["order_filled_events", "bot_commands"]).await {
        error!("[RedisDispatcher] FATAL: Failed to subscribe to channels: {}", e);
        return;
    }
    info!("[RedisDispatcher] Successfully subscribed. Waiting for messages.");

    let mut stream = pubsub.on_message();
    let shutdown_notified = shutdown.notified().fuse();
    futures::pin_mut!(shutdown_notified);

    loop {
        tokio::select! {
            Some(msg) = stream.next() => {
                trace!("[RedisDispatcher] Received message from Redis Pub/Sub.");
                let channel = msg.get_channel_name();
                let payload: Result<String, _> = msg.get_payload();

                if let Ok(payload_str) = payload {
                    match channel {
                        "order_filled_events" => {
                            if let Ok(event) = serde_json::from_str::<OrderFilledEvent>(&payload_str) {
                                if order_filled_tx.send(event).await.is_err() {
                                    warn!("[RedisDispatcher] Failed to send OrderFilledEvent to PositionManager (channel closed).");
                                }
                            } else {
                                warn!("[RedisDispatcher] Failed to parse OrderFilledEvent: {}", payload_str);
                            }
                        },
                        "bot_commands" => {
                            if let Ok(cmd) = serde_json::from_str::<WsCommand>(&payload_str) {
                                if bot_command_tx.send(cmd).await.is_err() {
                                    warn!("[RedisDispatcher] Failed to send WsCommand to CommandListener (channel closed).");
                                }
                            } else {
                                warn!("[RedisDispatcher] Failed to parse WsCommand: {}", payload_str);
                            }
                        },
                        _ => {
                            trace!("[RedisDispatcher] Received message on unhandled channel: {}", channel);
                        }
                    }
                }
            },
            _ = &mut shutdown_notified => {
                info!("[RedisDispatcher] Shutdown signal received. Exiting.");
                break;
            }
        }
    }
    warn!("[RedisDispatcher] Message stream ended. Task is finishing.");
}

/// Listens for commands on a Redis Pub/Sub channel and updates the application state.
async fn command_listener_task(
    app_state: Arc<AppState>,
    mut command_rx: mpsc::Receiver<WsCommand>,
    shutdown: Arc<Notify>,
) {
    info!("[CommandListener] Task starting. Waiting for commands from dispatcher.");

    loop {
        tokio::select! {
            Some(command) = command_rx.recv() => {
                match command.action.as_str() {
                    "graceful_stop" => {
                        info!("[CommandListener] Received 'graceful_stop' command. Setting status to Stopping.");
                        app_state.inner.trading_status.store(TradingStatus::Stopping as u8, Ordering::SeqCst);
                    },
                    "force_close" => {
                        if let Some(symbol) = command.symbol {
                            info!("[CommandListener] Received 'force_close' command for {}.", &symbol);
                            app_state.inner.force_close_requests.insert(symbol);
                        } else {
                            warn!("[CommandListener] 'force_close' command received without a symbol.");
                        }
                    },
                    "force_close_all" => {
                        info!("[CommandListener] Received 'force_close_all' command. Flagging all active positions for closure.");
                        // Проходим по всем активным позициям и добавляем их в `force_close_requests`
                        for entry in app_state.inner.active_positions.iter() {
                            app_state.inner.force_close_requests.insert(entry.key().clone());
                        }
                    },
                    _ => {
                        warn!("[CommandListener] Received unknown command: {}", command.action);
                    }
                }
            },
            _ = shutdown.notified() => {
                info!("[CommandListener] Shutdown signal received. Exiting.");
                break;
            }
        }
    }
    warn!("[CommandListener] Command channel closed. Task is finishing.");
}