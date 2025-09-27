// src/algorithm.rs

use crate::api_client::{ApiClient, PlaceFuturesOrderRequest, PlaceOrderRequest};
use crate::config::Config;
use crate::order_watcher::{OrderType, WatchOrderRequest};
use crate::state::AppState;
use crate::trading_logic;
use crate::types::{ActivePosition, ArbitrageDirection, CompletedTrade, PairData, TradingStatus};
use chrono::Utc;
use rust_decimal::Decimal;
use std::sync::atomic::{Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// Запускается в отдельной задаче и периодически проверяет рыночные данные на наличие арбитражных возможностей.
pub async fn run_trading_algorithm(
    app_state: Arc<AppState>,
    config: Arc<Config>, // <-- ИЗМЕНЕНИЕ: Принимаем Arc<Config>
    api_client: Arc<ApiClient>,
    order_watch_tx: mpsc::Sender<WatchOrderRequest>,
    mut orderbook_update_rx: mpsc::Receiver<String>, // <-- Принимаем Receiver
    shutdown: Arc<tokio::sync::Notify>, // <-- Новый аргумент
) {
    info!("Trading algorithm started. Waiting for order book updates...");

    // --- ОСНОВНОЙ СОБЫТИЙНЫЙ ЦИКЛ ---
    loop {
        tokio::select! {
            // Ветка 1: Выполняем работу по событию
            Some(symbol) = orderbook_update_rx.recv() => {
                // Как только пришел сигнал, что пара `symbol` обновилась,
                // мы немедленно запускаем для нее проверку в отдельной задаче.

                tokio::spawn({
                    // Клонируем все необходимое для задачи
                    let app_state = app_state.clone();
                    let config = config.clone(); // <-- ИЗМЕНЕНИЕ: Клонируем Arc, это дешево
                    let api_client = api_client.clone();
                    let order_watch_tx = order_watch_tx.clone();

                    async move {
                        // Троттлинг
                        let throttle_duration = Duration::from_millis(50); // Проверять одну пару не чаще, чем раз в 50 мс

                        if let Some(last_check_time) = app_state.inner.last_checked.get(&symbol) {
                            if last_check_time.elapsed() < throttle_duration {
                                return; // Слишком рано, пропускаем этот сигнал
                            }
                        }
                        // Если проверки не было или время прошло, обновляем метку
                        app_state.inner.last_checked.insert(symbol.clone(), Instant::now());


                        // ПРОВЕРЯЕМ ТОЛЬКО ОДНУ, ОБНОВИВШУЮСЯ ПАРУ

                        // Сначала извлекаем данные, чтобы освободить заимствования
                        let position_to_handle = app_state.inner.active_positions.get(&symbol).map(|p| p.value().clone());
                        let pair_data_to_handle = app_state.inner.market_data.get(&symbol).map(|pd| pd.value().clone());

                        // Проверяем, есть ли по ней открытая позиция
                        if let Some(position) = position_to_handle {
                            // Если да, запускаем логику закрытия
                            handle_open_position(
                                &symbol,
                                position,
                                app_state.clone(),
                                config.clone(), // Клонируем Arc
                                api_client.clone(),
                                order_watch_tx.clone(),
                            ).await;
                        } else {
                            // Если позиции нет, проверяем возможность открытия новой
                            if let Some(pair_data) = pair_data_to_handle {
                                handle_unopened_pair(
                                    &symbol, pair_data, app_state, config, api_client, order_watch_tx,
                                )
                                .await;
                            }
                        }
                    } // <-- Закрывается async move
                }); // <-- Закрывается tokio::spawn
            },
            // Ветка 2: Ждем сигнала об остановке
            _ = shutdown.notified() => {
                info!("[Algorithm] Shutdown signal received. Exiting.");
                break; // Выходим из `loop`
            }
        }
    }
}

/// Обрабатывает одну открытую позицию: обновляет ее состояние и проверяет условия выхода.
async fn handle_open_position(
    symbol: &str,
    position: ActivePosition,
    app_state: Arc<AppState>,
    config: Arc<Config>, // <-- ИЗМЕНЕНИЕ: Принимаем Arc<Config>
    api_client: Arc<ApiClient>,
    order_watch_tx: mpsc::Sender<WatchOrderRequest>,
) {
    // Для проверки выхода нам нужны актуальные рыночные данные
    let pair_data = match app_state.inner.market_data.get(symbol) {
        Some(data) => data,
        None => return,
    };

    // Рассчитываем параметры для симуляции закрытия
    let (r_exit_opt, c_exit_opt) = match position.direction {
        ArbitrageDirection::BuySpotSellFutures => (
            trading_logic::calculate_revenue_from_sale(&pair_data.spot_book.bids, position.base_qty),
            trading_logic::calculate_cost_to_acquire(&pair_data.futures_book.asks, position.base_qty),
        ),
        _ => (None, None),
    };

    if let (Some(r_exit_res), Some(c_exit_res)) = (r_exit_opt, c_exit_opt) {
        let revenue_spot_exit = r_exit_res.total_quote_qty;
        let cost_futures_exit = c_exit_res.total_quote_qty;
 
        // --- УПРОЩЕННАЯ ПРОВЕРКА ---
        let close_reason: Option<&'static str> = 
            if app_state.inner.force_close_requests.remove(symbol).is_some() {
                Some("Force Closed by User")
            } else if revenue_spot_exit >= cost_futures_exit { // Простое условие безубыточности
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
 
                 // Генерируем уникальный ID для закрывающих ордеров
                let client_oid = uuid::Uuid::new_v4().to_string(); // Один ID для пары

                // Формируем ордера на закрытие: продаем спот и откупаем фьючерс
                // --- НАЧАЛО: Логика округления для закрытия ---
                let rules = app_state.inner.symbol_rules.get(symbol)
                    .map(|r| *r.value())
                    .unwrap_or_default();

                // Для market-sell на споте `size` - это объем в базовой валюте.
                let spot_close_qty = if let Some(scale) = rules.spot_quantity_scale {
                    trading_logic::round_down(position.base_qty, scale)
                } else {
                    warn!("[{}] Spot quantity scale not found for closing. Using unrounded value.", symbol);
                    position.base_qty
                };

                // Для market-buy на фьючерсах `size` - это объем в базовой валюте.
                let futures_close_qty = if let Some(scale) = rules.futures_quantity_scale {
                    trading_logic::round_down(position.base_qty, scale)
                } else {
                    warn!("[{}] Futures quantity scale not found for closing. Using unrounded value.", symbol);
                    position.base_qty
                };
                // --- КОНЕЦ: Логика округления для закрытия ---

                let spot_order_req = PlaceOrderRequest {
                    symbol: position.symbol.clone(),
                    side: "sell".to_string(),
                    order_type: "market".to_string(),
                    size: spot_close_qty.to_string(), // Используем округленный объем
                    client_oid: Some(client_oid.clone()),
                };
                let futures_order_req = PlaceFuturesOrderRequest {
                    symbol: position.symbol.clone(),
                    product_type: "USDT-FUTURES".to_string(),
                    margin_mode: "isolated".to_string(),
                    margin_coin: "USDT".to_string(),
                    size: futures_close_qty.to_string(), // Используем округленный объем
                    side: "buy".to_string(),
                    trade_side: None, // Для one_way_mode
                    order_type: "market".to_string(),
                    client_oid: Some(client_oid.clone()),
                };

                // Запускаем исполнение в отдельной задаче
                tokio::spawn({
                    let client = api_client.clone();
                    let order_tx = order_watch_tx.clone();
                    let symbol = symbol.to_string();
                    let app_state = app_state.clone();
 
                    async move {
                        let (spot_res, fut_res) = tokio::join!(
                            client.place_spot_order(spot_order_req),
                            client.place_futures_order(futures_order_req)
                        );
 
                        match (&spot_res, &fut_res) {
                            (Ok(spot_ord), Ok(fut_ord)) => {
                                info!("[{}] Successfully sent CLOSE orders.", &symbol);
                                // Отправляем на отслеживание с контекстом Exit
                                let spot_watch_req = WatchOrderRequest {
                                    symbol: symbol.clone(),
                                    order_id: spot_ord.order_id.clone(), // Используем один client_oid для сопоставления
                                    order_type: OrderType::Spot, // Используем один client_oid для сопоставления
                                    client_oid: client_oid.clone(),
                                    context: crate::order_watcher::OrderContext::Exit,
                                };
                                let fut_watch_req = WatchOrderRequest {
                                    symbol: symbol.clone(),
                                    order_id: fut_ord.order_id.clone(),
                                    order_type: OrderType::Futures,
                                    client_oid: client_oid.clone(),
                                    context: crate::order_watcher::OrderContext::Exit,
                                }; 
 
                                if order_tx.send(spot_watch_req).await.is_err() {
                                    error!("[{}] Failed to send CLOSE spot order to watcher.", &symbol);
                                    // TODO: Критическая ошибка, нужна логика компенсации
                                    app_state.inner.executing_pairs.remove(&symbol);
                                }
                                if order_tx.send(fut_watch_req).await.is_err() {
                                    error!("[{}] Failed to send CLOSE futures order to watcher.", &symbol);
                                    // TODO: Критическая ошибка, нужна логика компенсации
                                    app_state.inner.executing_pairs.remove(&symbol);
                                }
                                // `PositionManager` обработает исполнение, удалит `ActivePosition`
                                // и снимет блокировку `executing_pairs`.
                            },
                            _ => {
                                error!("[{}] FAILED to send one or both CLOSE orders. Will retry. Spot: {:?}, Futures: {:?}", &symbol, spot_res, fut_res);
                                // Снимаем блокировку, чтобы можно было попробовать снова
                                app_state.inner.executing_pairs.remove(&symbol);
                            }
                        }
                    }
                });
            } else {
                // *** ВИРТУАЛЬНЫЙ РЕЖИМ ***
                if let Some((_key, pos_to_close)) = app_state.inner.active_positions.remove(symbol) {
                    // Расчеты PnL и прочего теперь будут в PositionManager или при сохранении
                    let completed_trade = CompletedTrade {
                        entry_data: pos_to_close.clone(),
                        exit_time: Utc::now().timestamp_millis(),
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
    }
}

/// Обрабатывает одну пару без открытой позиции: проверяет лимиты и запускает исполнителя.
async fn handle_unopened_pair(
    symbol: &str,
    pair_data: PairData,
    app_state: Arc<AppState>,
    config: Arc<Config>,
    api_client: Arc<ApiClient>,
    order_watch_tx: mpsc::Sender<WatchOrderRequest>,
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
    if app_state.inner.executing_pairs.contains(symbol) {
        return;
    }

    let symbol_owned = symbol.to_string(); // Convert &str to an owned String
    // --- Если все проверки пройдены, запускаем задачу на проверку спреда и исполнение ---
    tokio::spawn(async move {
         check_and_execute_arbitrage(
             &symbol_owned,
             &pair_data,
             app_state,
             config, // <-- ПЕРЕДАЕМ Arc ПО ВЛАДЕНИЮ
             api_client,
             &order_watch_tx,
         ).await;
    });
}

/// Проверяет спред и, если он выгодный, БЛОКИРУЕТ пару и пытается исполнить сделку.
async fn check_and_execute_arbitrage(
    symbol: &str,
    pair_data: &PairData,
    app_state: Arc<AppState>,
    config: Arc<Config>, // <-- ИЗМЕНЕНИЕ: Принимаем Arc<Config>
    api_client: Arc<ApiClient>,
    order_watch_tx: &mpsc::Sender<WatchOrderRequest>,
) {
    let hundred = Decimal::from(100);

    // 1. Берем лучшую цену из стакана фьючерсов для расчета N.
    // Это надежнее, чем ждать last_price из канала trades.
    let last_price = match pair_data.futures_book.asks.iter().next() {
        Some((&price, _)) if !price.is_zero() => price,
        _ => return, // Если стакан пуст или цена нулевая, выходим
    };
    let base_qty_n = config.trade_amount_usdt / last_price;

    // 2. Рассчитываем VWAP для этого N
    if let Some(sell_futures_res) = trading_logic::calculate_revenue_from_sale(&pair_data.futures_book.bids, base_qty_n) {
        let revenue_futures_entry = sell_futures_res.total_quote_qty;

        if let Some(buy_spot_res) =
            trading_logic::calculate_cost_to_acquire(&pair_data.spot_book.asks, base_qty_n)
        {
            let cost_spot_entry = buy_spot_res.total_quote_qty;

            if !cost_spot_entry.is_zero() {
                // 3. Проверяем спред
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

                    // Получаем правила для символа. Если их нет, используем `default()`.
                    let rules = app_state.inner.symbol_rules.get(symbol)
                        .map(|r| *r.value())
                        .unwrap_or_default();

                    // --- НАЧАЛО: Логика округления для входа ---
                    // Для market-buy на споте `size` - это стоимость в quote (USDT).
                    let rounded_spot_cost = if let Some(scale) = rules.spot_price_scale {
                        trading_logic::round_down(cost_spot_entry, scale)
                    } else {
                        warn!("[{}] Spot price scale not found. Using unrounded value for entry cost.", symbol);
                        cost_spot_entry
                    };

                    // Для market-sell на фьючерсах `size` - это объем в базовой валюте.
                    let rounded_futures_qty = if let Some(scale) = rules.futures_quantity_scale {
                        trading_logic::round_down(base_qty_n, scale)
                    } else {
                        warn!("[{}] Futures quantity scale not found. Using unrounded value for entry.", symbol);
                        base_qty_n
                    };
                    // --- КОНЕЦ: Логика округления для входа ---

                    info!("[{}] ARBITRAGE OPPORTUNITY DETECTED. Gross Spread: {:.4}%. Base Qty (N): {}. Locking pair for execution...", symbol, spread_percent, rounded_futures_qty);

                    if config.live_trading_enabled {
                        // *** БОЕВОЙ РЕЖИМ ***
                        let spot_order_task = tokio::spawn({
                            let client = api_client.clone();
                            let symbol = symbol.to_string();
                            let client_oid = client_oid.clone();
                            let rounded_spot_cost_str = rounded_spot_cost.to_string();
                            async move {
                                let order = PlaceOrderRequest {
                                    symbol: symbol.clone(),
                                    side: "buy".to_string(),
                                    order_type: "market".to_string(),
                                    size: rounded_spot_cost_str, // ИСПОЛЬЗУЕМ ОКРУГЛЕННОЕ
                                    client_oid: Some(client_oid),
                                };
                                client.place_spot_order(order).await
                            }
                        });
                        let futures_order_task = tokio::spawn({
                            let client = api_client.clone();
                            let symbol = symbol.to_string();
                            let rounded_futures_qty_str = rounded_futures_qty.to_string();
                            let client_oid = client_oid.clone();
                            async move {
                                let order = PlaceFuturesOrderRequest {
                                    symbol: symbol.clone(),
                                    product_type: "USDT-FUTURES".to_string(),
                                    margin_mode: "isolated".to_string(),
                                    margin_coin: "USDT".to_string(),
                                    size: rounded_futures_qty_str, // ИСПОЛЬЗУЕМ ОКРУГЛЕННОЕ
                                    side: "sell".to_string(),
                                    trade_side: None, // <-- ИЗМЕНЕНИЕ: Не указываем в one-way mode
                                    order_type: "market".to_string(),
                                    client_oid: Some(client_oid),
                                };
                                client.place_futures_order(order).await
                            }
                        });
                        let (spot_result, futures_result) =
                            tokio::join!(spot_order_task, futures_order_task);

                        match (spot_result, futures_result) {
                            (Ok(Ok(spot_order)), Ok(Ok(futures_order))) => {
                                info!("[{}] Successfully sent both orders to API. Spot ID: {}, Futures ID: {}", symbol, &spot_order.order_id, &futures_order.order_id);
                                // Отправляем ордера на отслеживание
                                let spot_watch_req = WatchOrderRequest {
                                    symbol: symbol.to_string(),
                                    order_id: spot_order.order_id.clone(),
                                    order_type: OrderType::Spot,
                                    client_oid: client_oid.clone(), // Указываем контекст ВХОДА
                                    context: crate::order_watcher::OrderContext::Entry,
                                };
                                let futures_watch_req = WatchOrderRequest {
                                    symbol: symbol.to_string(),
                                    order_id: futures_order.order_id.clone(),
                                    order_type: OrderType::Futures,
                                    client_oid: client_oid.clone(), // Указываем контекст ВХОДА
                                    context: crate::order_watcher::OrderContext::Entry,
                                };

                                if order_watch_tx.send(spot_watch_req).await.is_err() {
                                    error!("[{}] Failed to send SPOT order {} to watcher.", symbol, &spot_order.order_id);
                                    // TODO: Критическая ошибка, нужна логика компенсации (отмена фьючерсного ордера)
                                }
                                if order_watch_tx.send(futures_watch_req).await.is_err() {
                                    error!("[{}] Failed to send futures order {} to watcher.", symbol, &futures_order.order_id);
                                     // TODO: Критическая ошибка, нужна логика компенсации (отмена спотового ордера)
                                     app_state.inner.executing_pairs.remove(symbol);
                                }
                            },
                            // --- НОВЫЙ БЛОК: Обработка частичного исполнения ---
                            (Ok(Ok(spot_order)), Err(e_fut)) => {
                                error!("[{}] LEGGING RISK: Placed SPOT order {} but FAILED to place FUTURES order: {:?}. Attempting to close spot leg immediately!", symbol, &spot_order.order_id, e_fut);
                                
                                // Немедленно отправляем ордер на продажу спота, чтобы закрыть позицию
                                // ВАЖНО: Мы не знаем точный объем исполнения market-buy ордера.
                                // Полноценная компенсация потребует получения исполненного объема (через order watcher)
                                // и затем продажи этого объема.
                                // Здесь мы просто логируем и снимаем блокировку.
                                // В будущем здесь будет вызов компенсирующей логики.
                                
                                app_state.inner.executing_pairs.remove(symbol); // Снимаем блокировку
                            },
                            (Err(e_spot), Ok(Ok(futures_order))) => {
                                error!("[{}] LEGGING RISK: Placed FUTURES order {} but FAILED to place SPOT order: {:?}. Attempting to close futures leg immediately!", symbol, &futures_order.order_id, e_spot);

                                // Немедленно отправляем ордер на откуп фьючерса
                                let compensation_order = PlaceFuturesOrderRequest {
                                    symbol: symbol.to_string(),
                                    product_type: "USDT-FUTURES".to_string(),
                                    margin_mode: "isolated".to_string(),
                                    margin_coin: "USDT".to_string(),
                                    size: rounded_futures_qty.to_string(), // Используем округленный объем
                                    side: "buy".to_string(),
                                    trade_side: None, // Для one-way_mode
                                    order_type: "market".to_string(),
                                    client_oid: None, // Не нужен, т.к. это не часть стандартной сделки
                                };
                                let _ = api_client.place_futures_order(compensation_order).await;

                                app_state.inner.executing_pairs.remove(symbol);
                            },
                            // --- КОНЕЦ НОВОГО БЛОКА ---
                            (spot_res, fut_res) => {
                                error!("[{}] FAILED to place both orders. Spot: {:?}, Futures: {:?}. Releasing lock.", symbol, spot_res, fut_res);
                                // ВАЖНО: Если исполнение не удалось, СРАЗУ снимаем блокировку
                                app_state.inner.executing_pairs.remove(symbol);
                            }
                        }
                    } else {
                        // Virtual trading logic
                        info!("[{}] VIRTUAL TRADE: Arbitrage opportunity found. Spread: {:.4}%", symbol, spread_percent);
                        // ВАЖНО: В конце виртуальной сделки тоже нужно снять блокировку
                        app_state.inner.executing_pairs.remove(symbol);
                    }
                }
            }
        }
    }
}