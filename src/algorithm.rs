// /home/platon/Bot_Bit_Get_Rust/src/algorithm.rs

use crate::api_client::{ApiClient, PlaceFuturesOrderRequest, PlaceOrderRequest};
use crate::config::Config;
use crate::order_watcher::{OrderType, WatchOrderRequest};
use crate::state::AppState;
use crate::trading_logic;
use crate::types::{ActivePosition, ArbitrageDirection, CompletedTrade, CompensationTask, PairData, TradingStatus};
use crate::utils::send_cancellable; //
use futures_util::{future::Either, FutureExt};
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::atomic::{Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant}; // No change needed here, but for context
use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// Runs the main trading algorithm loop. This is the "hot path".
/// It synchronously processes market data updates to minimize latency.
pub async fn run_trading_algorithm(
    app_state: Arc<AppState>,
    config: Arc<Config>,
    api_client: Arc<ApiClient>,
    order_watch_tx: mpsc::Sender<WatchOrderRequest>,
    compensation_tx: mpsc::Sender<CompensationTask>,
    mut orderbook_update_rx: mpsc::Receiver<String>,
    shutdown: Arc<tokio::sync::Notify>,
) {
    info!("Trading algorithm started. Waiting for order book updates...");

    // --- HOT PATH EVENT LOOP ---
    loop {
        tokio::select! {
            // Branch 1: Process a market data update.
            Some(symbol) = orderbook_update_rx.recv() => {
                // Throttling check
                let throttle_duration = Duration::from_millis(config.throttle_ms);
                if let Some(last_check_time) = app_state.inner.last_checked.get(&symbol) {
                    if last_check_time.elapsed() < throttle_duration {
                        continue; // Too soon, skip this update.
                    }
                }
                app_state.inner.last_checked.insert(symbol.clone(), Instant::now());

                // --- All logic below is now synchronous within the hot path ---

                // We clone Arcs which is cheap.
                let app_state_clone = app_state.clone();
                let config_clone = config.clone();
                let api_client_clone = api_client.clone();
                let order_watch_tx_clone = order_watch_tx.clone();
                let compensation_tx_clone = compensation_tx.clone();

                let shutdown_clone = shutdown.clone();

                // Check if there's an open position for the symbol.
                if let Some(position) = app_state.inner.active_positions.get(&symbol).map(|p| p.value().clone()) {
                    // If yes, spawn a task to handle the closing logic (less time-critical).
                    tokio::spawn(async move {
                        handle_open_position(&symbol, position, app_state_clone, config_clone, api_client_clone, order_watch_tx_clone, compensation_tx_clone, shutdown_clone).await;
                    });
                } else {
                    // If no position, check for a new opening opportunity. This is the most critical path.
                    if let Some(pair_data) = app_state.inner.market_data.get(&symbol) {
                        // --- ИЗМЕНЕНИЕ: Запускаем проверку в отдельной задаче, чтобы не блокировать основной цикл ---
                        tokio::spawn(handle_unopened_pair(symbol.clone(), pair_data.value().clone(), app_state_clone, config_clone, api_client_clone, order_watch_tx_clone, compensation_tx_clone, shutdown_clone));
                    }
                }
            },
            // Branch 2: Wait for a shutdown signal.
            _ = shutdown.notified() => {
                info!("[Algorithm] Shutdown signal received. Exiting.");
                break; // Выходим из `loop`
            }
        }

        // --- ПРОВЕРКА НА GRACEFUL STOP ---
        // Если установлен статус "Остановка" и все позиции закрыты, инициируем полное завершение.
        let status = app_state.inner.trading_status.load(Ordering::SeqCst);
        if status == TradingStatus::Stopping as u8 && app_state.inner.active_positions.is_empty() {
            info!("[Algorithm] Graceful stop complete: all positions are closed. Shutting down.");
            shutdown.notify_waiters();
            break; // Выходим из цикла алгоритма
        }
    }
}

/// Обрабатывает одну открытую позицию: обновляет ее состояние и проверяет условия выхода.
async fn handle_open_position(
    symbol: &str,
    position: ActivePosition, // Passed by value
    app_state: Arc<AppState>,
    config: Arc<Config>, // <-- ИЗМЕНЕНИЕ: Принимаем Arc<Config>
    api_client: Arc<ApiClient>,
    order_watch_tx: mpsc::Sender<WatchOrderRequest>,
    compensation_tx: mpsc::Sender<CompensationTask>,
    shutdown: Arc<tokio::sync::Notify>,
) {
    // Для проверки выхода нам нужны актуальные рыночные данные
    let pair_data = match app_state.inner.market_data.get(symbol) {
        Some(data) => data,
        None => return,
    };
 
    // Рассчитываем параметры для симуляции закрытия и обновления PnL
    let (r_exit_opt, c_exit_opt) = match position.direction {
        ArbitrageDirection::BuySpotSellFutures => (
            trading_logic::calculate_revenue_from_sale(&pair_data.spot_book.bids, position.spot_base_qty),
            trading_logic::calculate_cost_to_acquire(&pair_data.futures_book.asks, position.futures_base_qty),
        ),
        _ => (None, None),
    };

    if let (Some(r_exit_res), Some(c_exit_res)) = (r_exit_opt, c_exit_opt) {
        let revenue_spot_exit = r_exit_res.total_quote_qty;
        let cost_futures_exit = c_exit_res.total_quote_qty;
 
        // --- ПРОВЕРКА УСЛОВИЙ ВЫХОДА ---
        let close_reason: Option<&'static str> = 
            if app_state.inner.force_close_requests.remove(symbol).is_some() {
                Some("Force Closed by User")
            } else if revenue_spot_exit >= cost_futures_exit { // Условие безубыточности или лучше
                Some("Favorable Exit")
            } else {
                None
            };

        if let Some(reason) = close_reason {
            // --- НАЧАЛО: Логика разделения режимов для закрытия позиции ---
            if config.live_trading_enabled {
                // Атомарно блокируем пару, чтобы не пытаться закрыть ее дважды
                if !app_state.inner.executing_pairs.insert(symbol.to_string()) {
                    return;
                }
 
                info!("[{}] LIVE TRADING: Attempting to close position (Reason: {})...", symbol, reason);
 
                // Запускаем исполнение в отдельной задаче
                tokio::spawn({
                    let client = api_client.clone();
                    let order_tx = order_watch_tx.clone();
                    let symbol = symbol.to_string();
                    let compensation_tx = compensation_tx.clone();
                    let position = position.clone(); // Нам нужна позиция для данных
                    let app_state = app_state.clone();
                    let shutdown = shutdown.clone();
 
                    async move {
                        // --- НОВАЯ, ФИНАЛЬНАЯ ЛОГИКА ЗАКРЫТИЯ ---

                        // Получаем правила для символа ОДИН РАЗ перед циклом
                        let rules = app_state.inner.symbol_rules.get(&symbol)
                            .map(|r| *r.value())
                            .unwrap_or_default();

                        // Определяем порог "пыли". Если правило не загрузилось, используем безопасный фолбэк.
                        let dust_threshold = rules.min_trade_amount
                            .unwrap_or_else(|| Decimal::from_str("0.0001").unwrap());

                        // --- ЭТАП 1: Получаем ФАКТИЧЕСКИЙ спотовый баланс с ретраями ---
                        info!("[{}] Closing Step 1: Fetching actual spot balance...", symbol);
                        let base_coin = position.symbol.replace("USDT", "");
                        
                        let mut attempts = 0;
                        let actual_spot_balance = loop {
                            attempts += 1;
                            info!("[{}] Attempt #{} to fetch spot balance for {}", symbol, attempts, base_coin);

                            match client.get_spot_balance(&base_coin).await {
                                Ok(balance) => {
                                    if balance >= dust_threshold {
                                        info!("[{}] Closing Step 1 SUCCESS: Actual balance {} is >= min_trade_amount {}.", symbol, balance, dust_threshold);
                                        break balance; // Выходим из цикла, возвращая баланс
                                    } else {
                                        warn!("[{}] Fetched zero or dust balance ({} is < min_trade_amount {}). Retrying...", symbol, balance, dust_threshold);
                                        tokio::time::sleep(Duration::from_secs(1)).await;
                                    }
                                },
                                Err(e) => {
                                    error!("[{}] Could not fetch spot balance: {:?}. Retrying in 1 second...", symbol, e);
                                    tokio::time::sleep(Duration::from_secs(1)).await;
                                }
                            }

                            if attempts >= 10 { // Защита от вечного цикла
                                error!("[{}] Failed to fetch a valid spot balance after 10 attempts. Aborting close.", symbol);
                                app_state.inner.executing_pairs.remove(&symbol);
                                return;
                            }
                        };
 
                        // --- ЭТАП 2: Формируем и отправляем ордера ПАРАЛЛЕЛЬНО ---
                        info!("[{}] Closing Step 2: Sending close orders in parallel.", symbol);
                        let client_oid = uuid::Uuid::new_v4().to_string();
 
                        // --- ИСПРАВЛЕНИЕ: Используем правила округления для спота ---
                        // Если правила не найдены, используем безопасное значение по умолчанию (например, 2),
                        // но лучше убедиться, что правила загружаются для всех пар.
                        let quantity_scale = rules.spot_quantity_scale.unwrap_or(2); // Фолбэк на 2 знака

                        let spot_close_qty = actual_spot_balance.trunc_with_scale(quantity_scale);
                        
                        // --- КЛЮЧЕВАЯ ПОПРАВКА ---
                        // Откупаем на фьючерсах ровно столько, сколько продаем на споте, чтобы свести дельту в ноль.
                        let futures_close_qty = spot_close_qty;

                        // Если баланс спота нулевой, нам не нужно отправлять этот ордер.
                        let spot_task: Either<_, _> = if !spot_close_qty.is_zero() {
                            let spot_order_req = PlaceOrderRequest {
                                symbol: position.symbol.clone(),
                                side: "sell".to_string(),
                                order_type: "market".to_string(),
                                force: "gtc".to_string(),
                                size: spot_close_qty.to_string(),
                                client_oid: Some(client_oid.clone()),
                            };
                            client.place_spot_order(spot_order_req).left_future()
                        } else {
                            info!("[{}] Closing Step 2 SKIPPED: Zero available spot balance to close.", symbol);
                            futures::future::ready(Err(crate::error::AppError::LogicError("Zero spot balance".to_string()))).right_future()
                        };

                        let fut_task = {
                            let futures_order_req = PlaceFuturesOrderRequest {
                                symbol: position.symbol.clone(),
                                product_type: "USDT-FUTURES".to_string(),
                                margin_mode: "isolated".to_string(),
                                margin_coin: "USDT".to_string(),
                                size: futures_close_qty.to_string(),
                                side: "buy".to_string(),
                                trade_side: None,
                                order_type: "market".to_string(),
                                client_oid: Some(client_oid.clone()),
                            };
                            client.place_futures_order(futures_order_req)
                        };

                        let (spot_res, fut_res) = tokio::join!(spot_task, fut_task); // Запускаем параллельно

                        // --- ЭТАП 3: Обрабатываем результаты ---
                        // Логика компенсации остается такой же, как и раньше
                        match (spot_res, fut_res) {
                            (Ok(spot_ord), Ok(fut_ord)) => {
                                info!("[{}] Closing Step 3 SUCCESS: Both CLOSE orders placed.", &symbol);
                                let spot_watch_req = WatchOrderRequest { symbol: symbol.clone(), order_id: spot_ord.order_id.clone(), order_type: OrderType::Spot, client_oid: client_oid.clone(), context: crate::order_watcher::OrderContext::Exit };
                                let fut_watch_req = WatchOrderRequest { symbol: symbol.clone(), order_id: fut_ord.order_id.clone(), order_type: OrderType::Futures, client_oid: client_oid, context: crate::order_watcher::OrderContext::Exit };
                                let _ = send_cancellable(&order_tx, spot_watch_req, &shutdown).await;
                                let _ = send_cancellable(&order_tx, fut_watch_req, &shutdown).await;
                            },
                            (Err(e), Ok(fut_ord)) if e.to_string().contains("Zero spot balance") => {
                                info!("[{}] Closing Step 3 PARTIAL: Futures order placed, spot order skipped (zero balance).", &symbol);
                                let fut_watch_req = WatchOrderRequest { symbol: symbol.clone(), order_id: fut_ord.order_id.clone(), order_type: OrderType::Futures, client_oid: client_oid, context: crate::order_watcher::OrderContext::Exit };
                                let _ = send_cancellable(&order_tx, fut_watch_req, &shutdown).await;
                            },
                            (Err(e_spot), Ok(fut_ord)) => {
                                error!("[{}] LEGGING RISK ON CLOSE: Placed FUTURES order {} but FAILED to place SPOT order: {:?}. Delegating to compensator.", symbol, &fut_ord.order_id, e_spot);
                                let task = CompensationTask { symbol: symbol.clone(), original_order_id: fut_ord.order_id.clone(), base_qty_to_compensate: futures_close_qty, original_direction: ArbitrageDirection::BuySpotSellFutures, leg_to_compensate: OrderType::Spot };
                                if !send_cancellable(&compensation_tx, task, &shutdown).await {
                                    error!("[{}] CRITICAL: Failed to send CLOSE compensation task for SPOT leg!", symbol);
                                }
                            },
                            (Ok(spot_ord), Err(e_fut)) => {
                                error!("[{}] LEGGING RISK ON CLOSE: Placed SPOT order {} but FAILED to place FUTURES order: {:?}. Delegating to compensator.", symbol, &spot_ord.order_id, e_fut);                                
                                let task = CompensationTask { symbol: symbol.clone(), original_order_id: spot_ord.order_id.clone(), base_qty_to_compensate: spot_close_qty, original_direction: ArbitrageDirection::BuySpotSellFutures, leg_to_compensate: OrderType::Futures };
                                if !send_cancellable(&compensation_tx, task, &shutdown).await {
                                    error!("[{}] CRITICAL: Failed to send CLOSE compensation task for FUTURES leg!", symbol);
                                }
                            },
                            (Err(e_spot), Err(e_fut)) => {
                                error!("[{}] FAILED to send both CLOSE orders. Spot: {:?}, Futures: {:?}. Releasing lock to retry.", &symbol, e_spot, e_fut);
                                app_state.inner.executing_pairs.remove(&symbol);
                            }
                        }
                    }
                });
            } else {
                // *** ВИРТУАЛЬНЫЙ РЕЖИМ ***
                if let Some((_key, pos_to_close)) = app_state.inner.active_positions.remove(symbol) {
                    let completed_trade = CompletedTrade {
                        entry_data: pos_to_close.clone(),
                        exit_time: chrono::Utc::now().timestamp_millis(),
                        cost_exit: cost_futures_exit,
                        revenue_exit: revenue_spot_exit,
                        final_pnl: Decimal::ZERO, // Будет рассчитано позже
                        entry_spread_percent: pos_to_close.entry_spread_percent,
                        exit_spread_percent: Decimal::ZERO, // Будет рассчитано позже
                    };
                    app_state.inner.completed_trades.entry(symbol.to_string()).or_default().push(completed_trade);
                    info!(
                        "[{}] CLOSE VIRTUAL POSITION (Reason: {}).",
                        symbol, reason
                    );
                }
            }
            // --- КОНЕЦ: Логика разделения режимов для закрытия позиции ---
        }
        // Если условия выхода не выполнены, обновляем текущее состояние PnL/спреда
    }
}

/// Обрабатывает одну пару без открытой позиции: проверяет лимиты и запускает исполнителя.
async fn handle_unopened_pair(
    symbol: String, // <-- ИЗМЕНЕНИЕ: Принимаем String, чтобы владеть данными в задаче
    pair_data: PairData, // <-- ИЗМЕНЕНИЕ: Принимаем по значению, так как работаем в отдельной задаче
    app_state: Arc<AppState>,
    config: Arc<Config>,
    api_client: Arc<ApiClient>,
    order_watch_tx: mpsc::Sender<WatchOrderRequest>,
    compensation_tx: mpsc::Sender<CompensationTask>,
    shutdown: Arc<tokio::sync::Notify>,
) {
    // --- ПРОВЕРКА СТАТУСА ТОРГОВЛИ ---
    let status = app_state.inner.trading_status.load(Ordering::SeqCst);
    if status == TradingStatus::Stopping as u8 {
        return; // Режим остановки, не ищем новые сделки.
    }
    // --- КОНЕЦ ---

    // --- НОВАЯ ЕДИНАЯ ПРОВЕРКА ЛИМИТА ---
    let active_count = app_state.inner.active_positions.len();
    let executing_count = app_state.inner.executing_pairs.len();

    if (active_count + executing_count) >= config.max_active_positions {
        // Общий лимит активности достигнут, выходим
        return;
    }

    // --- ПРОВЕРКА БЛОКИРОВКИ "EXECUTING" ---
    if app_state.inner.executing_pairs.contains(&symbol) {
        return;
    }

    // If all checks pass, run the spread check and potential execution.
    // This is now a synchronous call that spawns an async task.
    check_and_execute_arbitrage(&symbol, &pair_data, app_state, config, api_client, order_watch_tx, compensation_tx, shutdown); // No .await
}

/// Checks the spread and, if profitable, locks the pair and attempts to execute the trade.
fn check_and_execute_arbitrage( // No longer async
    symbol: &str,
    pair_data: &PairData,
    app_state: Arc<AppState>,
    config: Arc<Config>, // <-- ИЗМЕНЕНИЕ: Принимаем Arc<Config>
    api_client: Arc<ApiClient>,
    order_watch_tx: mpsc::Sender<WatchOrderRequest>,
    compensation_tx: mpsc::Sender<CompensationTask>,
    shutdown: Arc<tokio::sync::Notify>,
) {
    let hundred = Decimal::from(100);

    // 1. Get the best ask price from the futures book to calculate N.
    let last_price = match pair_data.futures_book.asks.iter().next() {
        Some((&price, _)) if !price.is_zero() => price,
        _ => return, // If book is empty or price is zero, exit.
    };

    // 2. Calculate base quantity N and round it down to 4 decimal places.
    // The quantity is rounded down later when creating the order request.
    let base_qty_n = config.trade_amount_usdt / last_price;

    // 3. Calculate VWAP for this rounded N.
    if let Some(sell_futures_res) =
        trading_logic::calculate_revenue_from_sale(&pair_data.futures_book.bids, base_qty_n) {
        let revenue_futures_entry = sell_futures_res.total_quote_qty;

        if let Some(buy_spot_res) =
            trading_logic::calculate_cost_to_acquire(&pair_data.spot_book.asks, base_qty_n)
        {
            let cost_spot_entry = buy_spot_res.total_quote_qty;

            if !cost_spot_entry.is_zero() {
                // 4. Check the spread.
                let spread_percent =
                    ((revenue_futures_entry - cost_spot_entry) / cost_spot_entry) * hundred;

                if spread_percent >= config.spread_threshold_percent {
                    // --- МОМЕНТ ИСТИНЫ: СПРЕД НАЙДЕН ---
                    // 1. Атомарно пытаемся "занять" пару. Если не вышло - кто-то нас опередил.
                    if !app_state.inner.executing_pairs.insert(symbol.to_string()) {
                        return; // Другой поток уже обрабатывает эту пару.
                    }
                    // Генерируем уникальный ID для этой пары ордеров
                    let client_oid = uuid::Uuid::new_v4().to_string();

                    // --- ИСПРАВЛЕНИЕ: Используем динамические правила округления ---
                    let rules = app_state.inner.symbol_rules.get(symbol)
                        .map(|r| *r.value())
                        .unwrap_or_default();
                    let futures_quantity_scale = rules.futures_quantity_scale.unwrap_or(4); // Фолбэк на 4

                    // Для market-sell на фьючерсах `size` - это объем в базовой валюте.
                    // Жестко "отрезаем" все, что идет после 4-го знака.
                    let rounded_futures_qty = base_qty_n.trunc_with_scale(futures_quantity_scale);
                    // --- КОНЕЦ ИСПРАВЛЕНИЯ ---

                    info!("[{}] ARBITRAGE OPPORTUNITY DETECTED. Gross Spread: {:.4}%. Base Qty (N): {}. Locking pair for execution...", symbol, spread_percent, rounded_futures_qty);

                    if config.live_trading_enabled {
                        // --- KEY CHANGE: "Fire and forget" ---
                        tokio::spawn(execute_trade_task(
                            symbol.to_string(),
                            cost_spot_entry, rounded_futures_qty, client_oid, app_state, config, api_client, order_watch_tx, compensation_tx, shutdown
                        ));
                    } // In virtual mode, we do nothing and the lock will expire or be handled elsewhere.
                }
            }
        }
    }
}

// --- НОВАЯ ФУНКЦИЯ: execute_trade_task ---
async fn execute_trade_task( // This new function runs in the background
    symbol: String,
    cost_spot_entry: Decimal,
    rounded_futures_qty: Decimal,
    client_oid: String,
    app_state: Arc<AppState>,
    config: Arc<Config>,
    api_client: Arc<ApiClient>,
    order_watch_tx: mpsc::Sender<WatchOrderRequest>,
    compensation_tx: mpsc::Sender<CompensationTask>,
    shutdown: Arc<tokio::sync::Notify>,
) {
    // --- НОВАЯ ГЛОБАЛЬНАЯ ЛОГИКА ОКРУГЛЕНИЯ ДЛЯ СПОТА ---
    let cost_with_slippage = cost_spot_entry * (Decimal::ONE + config.spot_slippage_buffer_percent / Decimal::from(100));

    const GLOBAL_PRICE_SCALE: u32 = 4; // Глобальное правило: 4 знака

    // Жестко "отрезаем" все, что идет после 4-го знака.
    let rounded_spot_cost = cost_with_slippage.trunc_with_scale(GLOBAL_PRICE_SCALE);
    // --- КОНЕЦ НОВОЙ ЛОГИКИ ---

    // 1. Отправляем ордера параллельно
    let (spot_res, fut_res) = tokio::join!(
        async {
            let order = PlaceOrderRequest {
                symbol: symbol.clone(),
                side: "buy".to_string(),
                order_type: "market".to_string(),
                force: "gtc".to_string(),
                size: rounded_spot_cost.to_string(),
                client_oid: Some(client_oid.clone()),
            };
            info!("[{}] SPOT ORDER REQUEST BODY: {}", symbol, serde_json::to_string(&order).unwrap_or_default());
            api_client.place_spot_order(order).await
        },
        async {
            let order = PlaceFuturesOrderRequest {
                symbol: symbol.clone(),
                product_type: "USDT-FUTURES".to_string(),
                margin_mode: "isolated".to_string(),
                margin_coin: "USDT".to_string(),
                size: rounded_futures_qty.to_string(),
                side: "sell".to_string(),
                trade_side: None,
                order_type: "market".to_string(),
                client_oid: Some(client_oid.clone()),
            };
            info!("[{}] FUTURES ORDER REQUEST BODY: {}", symbol, serde_json::to_string(&order).unwrap_or_default());
            api_client.place_futures_order(order).await
        }
    );

    // 2. Обрабатываем результат
    match (spot_res, fut_res) {
        // --- СЦЕНАРИЙ 1: ПОЛНЫЙ УСПЕХ ---
        (Ok(spot_order), Ok(futures_order)) => {
            info!("[{}] Successfully placed both orders. Spot ID: {}, Futures ID: {}. Sending to watcher.", symbol, &spot_order.order_id, &futures_order.order_id);
            let spot_order_id = spot_order.order_id.clone(); // Клонируем перед move
            let futures_order_id = futures_order.order_id.clone(); // Клонируем перед move
            let spot_watch_req = WatchOrderRequest { symbol: symbol.clone(), order_id: spot_order.order_id, order_type: OrderType::Spot, client_oid: client_oid.clone(), context: crate::order_watcher::OrderContext::Entry };
            let futures_watch_req = WatchOrderRequest { symbol: symbol.clone(), order_id: futures_order.order_id, order_type: OrderType::Futures, client_oid: client_oid, context: crate::order_watcher::OrderContext::Entry };
            if !send_cancellable(&order_watch_tx, spot_watch_req, &shutdown).await {
                error!("[{}] Failed to send SPOT order to watcher. Order ID: {}. The lock will not be released automatically.", symbol, spot_order_id);
                // TODO: Здесь может потребоваться дополнительная логика компенсации, если watcher не принял ордер.
            }
            if !send_cancellable(&order_watch_tx, futures_watch_req, &shutdown).await {
                error!("[{}] Failed to send FUTURES order to watcher. Order ID: {}. The lock will not be released automatically.", symbol, futures_order_id);
                // TODO: Здесь может потребоваться дополнительная логика компенсации.
                app_state.inner.executing_pairs.remove(&symbol); // Снимаем блокировку, т.к. фьючерсный ордер не отслеживается
            }
        },
        // --- СЦЕНАРИЙ 2 (LEGGING RISK): СПОТ УСПЕХ, ФЬЮЧЕРС ОШИБКА ---
        (Ok(spot_order), Err(e_fut)) => {
            error!("[{}] LEGGING RISK: Placed SPOT order {} but FAILED to place FUTURES order: {:?}. Sending to compensator.", symbol, &spot_order.order_id, e_fut);
            let spot_order_id = spot_order.order_id.clone(); // Клонируем перед move
            // We need to compensate the successful spot order. The quantity to compensate is not known yet,
            // as the fill event hasn't happened. The best we can do is use the intended futures quantity,
            // as spot was bought to match it. A more advanced implementation would wait for the spot fill
            // to get the exact quantity. For now, this is the most logical value to use.
            let task = CompensationTask {
                symbol: symbol.clone(),
                original_order_id: spot_order.order_id,
                base_qty_to_compensate: rounded_futures_qty, // The intended quantity for the pair
                original_direction: ArbitrageDirection::BuySpotSellFutures,
                leg_to_compensate: OrderType::Spot,
            };
            if !send_cancellable(&compensation_tx, task, &shutdown).await {
                error!("[{}] CRITICAL: Failed to send compensation task for spot leg! Order ID: {}", symbol, spot_order_id);
            }
            app_state.inner.executing_pairs.remove(&symbol); // Unlock the pair
        },
        // --- СЦЕНАРИЙ 3 (LEGGING RISK): ФЬЮЧЕРС УСПЕХ, СПОТ ОШИБКА ---
        (Err(e_spot), Ok(futures_order)) => {
            error!("[{}] LEGGING RISK: Placed FUTURES order {} but FAILED to place SPOT order: {:?}. Sending to compensator.", symbol, &futures_order.order_id, e_spot);
            let futures_order_id = futures_order.order_id.clone(); // Клонируем перед move
            let task = CompensationTask {
                symbol: symbol.clone(),
                original_order_id: futures_order.order_id,
                base_qty_to_compensate: rounded_futures_qty, // This was the actual quantity sent in the successful order
                original_direction: ArbitrageDirection::BuySpotSellFutures,
                leg_to_compensate: OrderType::Futures,
            };
            if !send_cancellable(&compensation_tx, task, &shutdown).await {
                error!("[{}] CRITICAL: Failed to send compensation task for futures leg! Order ID: {}", symbol, futures_order_id);
            }
            app_state.inner.executing_pairs.remove(&symbol); // Unlock the pair
        },
        // --- СЦЕНАРИЙ 4: ОБА ОРДЕРА НЕУДАЧНЫ ---
        (Err(e_spot), Err(e_fut)) => {
             error!("[{}] FAILED to place both orders. Spot: {:?}, Futures: {:?}. Releasing lock.", symbol, e_spot, e_fut);
             app_state.inner.executing_pairs.remove(&symbol);
        },
    }
}
