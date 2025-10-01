// /home/platon/Bot_Bit_Get_Rust/src/algorithm.rs

use crate::api_client::{ApiClient, PlaceFuturesOrderRequest, PlaceOrderRequest};
use crate::config::Config;
use crate::order_watcher::{OrderType, WatchOrderRequest};
use crate::state::AppState;
use crate::trading_logic;
use crate::types::{ActivePosition, ArbitrageDirection, CompensationTask, PairData, TradingStatus};
use crate::utils::send_cancellable;
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::atomic::{Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// Runs the main trading algorithm loop.
pub async fn run_trading_algorithm(
    app_state: Arc<AppState>,
    config: Arc<Config>,
    api_client: Arc<ApiClient>,
    order_watch_tx: mpsc::Sender<WatchOrderRequest>,
    compensation_tx: mpsc::Sender<CompensationTask>,
    mut orderbook_update_rx: mpsc::Receiver<String>,
    shutdown: Arc<tokio::sync::Notify>,
) {
    info!("[Algorithm] Service started.");
    loop {
        tokio::select! {
            Some(symbol) = orderbook_update_rx.recv() => {
                // Throttle checks for a given symbol to avoid redundant processing.
                let throttle_duration = Duration::from_millis(config.throttle_ms);
                if let Some(last_check_time) = app_state.inner.last_checked.get(&symbol) {
                    if last_check_time.elapsed() < throttle_duration {
                        continue;
                    }
                }
                app_state.inner.last_checked.insert(symbol.clone(), Instant::now());

                // Spawn a task to handle the logic for this symbol off the hot path.
                tokio::spawn({
                    let app_state = app_state.clone();
                    let config = config.clone();
                    let api_client = api_client.clone();
                    let order_watch_tx = order_watch_tx.clone();
                    let compensation_tx = compensation_tx.clone();
                    let shutdown = shutdown.clone();
                    async move {
                        if let Some(position) = app_state.inner.active_positions.get(&symbol).map(|p| p.value().clone()) {
                            handle_open_position(&symbol, position, app_state, config, api_client, order_watch_tx, compensation_tx, shutdown).await;
                        } else {
                            if let Some(pair_data) = app_state.inner.market_data.get(&symbol) {
                                let app_state_clone = app_state.clone();
                                handle_unopened_pair(symbol, pair_data.value().clone(), app_state_clone, config, api_client, order_watch_tx, compensation_tx, shutdown).await;
                            }
                        }
                    }
                });
            },
            _ = shutdown.notified() => {
                info!("[Algorithm] Shutdown signal received. Exiting main loop.");
                break;
            }
        }

        // Check for graceful stop condition.
        let status = app_state.inner.trading_status.load(Ordering::SeqCst);
        if status == TradingStatus::Stopping as u8 && app_state.inner.active_positions.is_empty() {
            info!("[Algorithm] Graceful stop complete. Shutting down.");
            shutdown.notify_waiters();
            break;
        }
    }
    info!("[Algorithm] Service has shut down.");
}

/// Handles an open position: checks for exit conditions and executes the closing trade.
async fn handle_open_position(
    symbol: &str,
    _position: ActivePosition,
    app_state: Arc<AppState>,
    _config: Arc<Config>,
    api_client: Arc<ApiClient>,
    order_watch_tx: mpsc::Sender<WatchOrderRequest>,
    compensation_tx: mpsc::Sender<CompensationTask>,
    shutdown: Arc<tokio::sync::Notify>,
) {
    if app_state.inner.executing_pairs.contains(symbol) {
        return;
    }

    // --- STAGE 1: Concurrently fetch actual spot balance and futures position size ---
    let base_coin = symbol.replace("USDT", "");
    let (spot_balance_res, futures_pos_res) = tokio::join!(
        api_client.get_spot_balance(&base_coin),
        api_client.get_futures_position(symbol)
    );

    let (actual_spot_balance, actual_futures_position) = match (spot_balance_res, futures_pos_res) {
        (Ok(spot), Ok(fut)) => (spot, fut),
        (Err(e), _) => {
            warn!("[Algorithm] Failed to get spot balance for {}: {:?}. Skipping exit check.", symbol, e);
            return;
        }
        (_, Err(e)) => {
            warn!("[Algorithm] Failed to get futures position for {}: {:?}. Skipping exit check.", symbol, e);
            return;
        }
    };

    // --- STAGE 2: Determine the safe quantity to close ---
    // This is the core of the safety mechanism. We can only close the minimum of what we have on both legs.
    let close_quantity = actual_spot_balance.min(actual_futures_position);

    // --- STAGE 3: Check if the safe quantity is above the dust threshold ---
    let rules = app_state.inner.symbol_rules.get(symbol).map(|r| *r.value()).unwrap_or_default();
    // Using a safe fallback for min_trade_amount, though it should be fetched from the API.
    let min_trade_qty = rules.min_trade_amount.unwrap_or_else(|| Decimal::from_str("0.0001").unwrap());
    if close_quantity < min_trade_qty {
        // The tradeable amount is too small (dust), nothing to do.
        return;
    }

    // --- STAGE 4: Simulate the exit trade using the SAFE quantity ---
    let pair_data = match app_state.inner.market_data.get(symbol) {
        Some(data) => data.clone(),
        None => return,
    };
    let (r_exit_opt, c_exit_opt) = (
        trading_logic::calculate_revenue_from_sale(&pair_data.spot_book.bids, close_quantity),
        trading_logic::calculate_cost_to_acquire(&pair_data.futures_book.asks, close_quantity),
    );

    if let (Some(r_exit_res), Some(c_exit_res)) = (r_exit_opt, c_exit_opt) {
        let exit_spread_percent = if !c_exit_res.total_quote_qty.is_zero() {
            ((r_exit_res.total_quote_qty - c_exit_res.total_quote_qty) / c_exit_res.total_quote_qty) * Decimal::from(100)
        } else {
            Decimal::ZERO
        };

        info!(
            "[Position Health] Symbol: {}, Spot Balance: {:.4}, Futures Pos: {:.4}, Current Exit Spread: {:.4}%",
            symbol,
            actual_spot_balance,
            actual_futures_position,
            exit_spread_percent
        );
        // --- STAGE 5: Make a decision based on the simulation ---
        let close_reason = if app_state.inner.force_close_requests.remove(symbol).is_some() {
            Some("Force Closed by User")
        } else if r_exit_res.total_quote_qty >= c_exit_res.total_quote_qty {
            Some("Favorable Exit")
        } else {
            None
        };

        // --- STAGE 6: Execute if a decision was made ---
        if let Some(reason) = close_reason {
            if !app_state.inner.executing_pairs.insert(symbol.to_string()) { return; }
            info!("[{}] EXIT TRIGGERED. Reason: {}. Safe Close Qty: {}. Spot Balance: {}, Futures Pos: {}. Sim Exit Revenue: {:.4}, Sim Exit Cost: {:.4}",
                symbol, reason, close_quantity, actual_spot_balance, actual_futures_position, r_exit_res.total_quote_qty, c_exit_res.total_quote_qty);

            tokio::spawn({
                let client = api_client.clone();
                let order_tx = order_watch_tx.clone();
                let symbol = symbol.to_string();
                let compensation_tx = compensation_tx.clone();
                let app_state = app_state.clone();
                let shutdown = shutdown.clone();

                async move {
                    let rules = app_state.inner.symbol_rules.get(&symbol)
                        .map(|r| *r.value())
                        .unwrap_or_default();
                    let client_oid = uuid::Uuid::new_v4().to_string();

                    // --- Apply individual rounding rules to the safe quantity ---
                    let spot_quantity_scale = rules.spot_quantity_scale.unwrap_or(2);
                    let futures_quantity_scale = rules.futures_quantity_scale.unwrap_or(4);

                    let spot_close_qty = close_quantity.trunc_with_scale(spot_quantity_scale);
                    let futures_close_qty = close_quantity.trunc_with_scale(futures_quantity_scale);

                    // Place Spot Sell Order
                    let spot_task = {
                        let req = PlaceOrderRequest {
                            symbol: symbol.clone(),
                            side: "sell".to_string(),
                            order_type: "market".to_string(),
                            force: "gtc".to_string(),
                            size: spot_close_qty.to_string(),
                            client_oid: Some(client_oid.clone()),
                        };
                        client.place_spot_order(req)
                    };

                    // Place Futures Buy (to close short) Order
                    let fut_task = {
                        let req = PlaceFuturesOrderRequest {
                            symbol: symbol.clone(),
                            product_type: "USDT-FUTURES".to_string(),
                            margin_mode: "isolated".to_string(),
                            margin_coin: "USDT".to_string(),
                            size: futures_close_qty.to_string(),
                            side: "buy".to_string(),
                            trade_side: None,
                            order_type: "market".to_string(),
                            client_oid: Some(client_oid.clone()),
                        };
                        client.place_futures_order(req)
                    };

                    let (spot_res, fut_res) = tokio::join!(spot_task, fut_task);

                    match (spot_res, fut_res) {
                        (Ok(spot_ord), Ok(fut_ord)) => {
                            info!("[{}] CLOSE SUCCESS: Both close orders placed. Spot ID: {}, Futures ID: {}", &symbol, spot_ord.order_id, fut_ord.order_id);
                            let spot_watch_req = WatchOrderRequest { symbol: symbol.clone(), order_id: spot_ord.order_id, order_type: OrderType::Spot, client_oid: client_oid.clone(), context: crate::order_watcher::OrderContext::Exit };
                            let fut_watch_req = WatchOrderRequest { symbol: symbol.clone(), order_id: fut_ord.order_id, order_type: OrderType::Futures, client_oid: client_oid, context: crate::order_watcher::OrderContext::Exit };
                            if !send_cancellable(&order_tx, spot_watch_req, &shutdown).await { error!("[{}] CRITICAL: Failed to send SPOT CLOSE watch request!", symbol); }
                            if !send_cancellable(&order_tx, fut_watch_req, &shutdown).await { error!("[{}] CRITICAL: Failed to send FUTURES CLOSE watch request!", symbol); }
                        },
                        (Err(e_spot), Ok(fut_ord)) => {
                            error!("[{}] LEGGING RISK ON CLOSE (Spot Failed): {:?}. Delegating to compensator.", symbol, e_spot);
                            let task = CompensationTask { symbol: symbol.clone(), original_order_id: fut_ord.order_id, base_qty_to_compensate: futures_close_qty, original_direction: ArbitrageDirection::BuySpotSellFutures, leg_to_compensate: OrderType::Spot, is_entry: false };
                            if !send_cancellable(&compensation_tx, task, &shutdown).await { error!("[{}] CRITICAL: Failed to send CLOSE compensation task for SPOT leg!", symbol); }
                        },
                        (Ok(spot_ord), Err(e_fut)) => {
                            error!("[{}] LEGGING RISK ON CLOSE (Futures Failed): {:?}. Delegating to compensator.", symbol, e_fut);
                            let task = CompensationTask { symbol: symbol.clone(), original_order_id: spot_ord.order_id, base_qty_to_compensate: spot_close_qty, original_direction: ArbitrageDirection::BuySpotSellFutures, leg_to_compensate: OrderType::Futures, is_entry: false };
                            if !send_cancellable(&compensation_tx, task, &shutdown).await { error!("[{}] CRITICAL: Failed to send CLOSE compensation task for FUTURES leg!", symbol); }
                        },
                        (Err(e_spot), Err(e_fut)) => {
                            error!("[{}] FAILED to send both CLOSE orders. Spot: {:?}, Futures: {:?}. Releasing lock to retry.", &symbol, e_spot, e_fut);
                            app_state.inner.executing_pairs.remove(&symbol);
                        }
                    }
                }
            });
        }
    }
}

/// Handles a pair with no open position: checks limits and spawns an execution task if profitable.
async fn handle_unopened_pair(
    symbol: String,
    pair_data: PairData,
    app_state: Arc<AppState>,
    config: Arc<Config>,
    api_client: Arc<ApiClient>,
    order_watch_tx: mpsc::Sender<WatchOrderRequest>,
    compensation_tx: mpsc::Sender<CompensationTask>,
    shutdown: Arc<tokio::sync::Notify>,
) {
    if app_state.inner.trading_status.load(Ordering::SeqCst) == TradingStatus::Stopping as u8 { return; }
    let active_count = app_state.inner.active_positions.len();
    let executing_count = app_state.inner.executing_pairs.len();
    if (active_count + executing_count) >= config.max_active_positions { return; }
    if app_state.inner.executing_pairs.contains(&symbol) { return; }

    check_and_execute_arbitrage(&symbol, &pair_data, app_state, config, api_client, order_watch_tx, compensation_tx, shutdown);
}

/// Synchronously checks for an arbitrage opportunity and spawns a background task to execute it.
fn check_and_execute_arbitrage(
    symbol: &str,
    pair_data: &PairData,
    app_state: Arc<AppState>,
    config: Arc<Config>,
    api_client: Arc<ApiClient>,
    order_watch_tx: mpsc::Sender<WatchOrderRequest>,
    compensation_tx: mpsc::Sender<CompensationTask>,
    shutdown: Arc<tokio::sync::Notify>,
) {
    // Мы собираемся ПРОДАТЬ фьючерсы, поэтому для расчета объема берем лучшую цену BID.
    let last_price = match pair_data.futures_book.bids.iter().rev().next() { // .rev() чтобы взять самую высокую цену bid
        Some((&price, _)) if !price.is_zero() => price,
        _ => return,
    };
    let base_qty_n = config.trade_amount_usdt / last_price;

    if let (Some(sell_futures_res), Some(buy_spot_res)) = (
        trading_logic::calculate_revenue_from_sale(&pair_data.futures_book.bids, base_qty_n),
        trading_logic::calculate_cost_to_acquire(&pair_data.spot_book.asks, base_qty_n),
    ) {
        if buy_spot_res.total_quote_qty.is_zero() { return; }
        let spread_percent = ((sell_futures_res.total_quote_qty - buy_spot_res.total_quote_qty) / buy_spot_res.total_quote_qty) * Decimal::from(100);

        if spread_percent >= config.spread_threshold_percent {
            if !app_state.inner.executing_pairs.insert(symbol.to_string()) { return; }
            
            let rules = app_state.inner.symbol_rules.get(symbol).map(|r| *r.value()).unwrap_or_default();
            let fut_scale = rules.futures_quantity_scale.unwrap_or(4);
            let rounded_futures_qty = base_qty_n.trunc_with_scale(fut_scale);

            info!("[{}] ENTRY TRIGGERED. Gross Spread: {:.4}%. Base Qty (N): {}. Spawning execution task...", symbol, spread_percent, rounded_futures_qty);

            if config.live_trading_enabled {
                tokio::spawn(execute_entry_trade_task(
                    symbol.to_string(),
                    buy_spot_res.total_quote_qty,
                    rounded_futures_qty,
                    uuid::Uuid::new_v4().to_string(),
                    app_state,
                    config,
                    api_client,
                    order_watch_tx,
                    compensation_tx,
                    shutdown
                ));
            } else {
                app_state.inner.executing_pairs.remove(symbol);
            }
        }
    }
}

/// Background task to place the entry orders.
async fn execute_entry_trade_task(
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
    let cost_with_slippage = cost_spot_entry * (Decimal::ONE + config.spot_slippage_buffer_percent / Decimal::from(100));
    const GLOBAL_PRICE_SCALE: u32 = 4;
    let rounded_spot_cost = cost_with_slippage.trunc_with_scale(GLOBAL_PRICE_SCALE);

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

    match (spot_res, fut_res) {
        (Ok(spot_order), Ok(futures_order)) => {
            info!("[{}] ENTRY SUCCESS: Both entry orders placed. Spot ID: {}, Futures ID: {}", &symbol, &spot_order.order_id, &futures_order.order_id);
            let spot_req = WatchOrderRequest { symbol: symbol.clone(), order_id: spot_order.order_id, order_type: OrderType::Spot, client_oid: client_oid.clone(), context: crate::order_watcher::OrderContext::Entry };
            let fut_req = WatchOrderRequest { symbol: symbol.clone(), order_id: futures_order.order_id, order_type: OrderType::Futures, client_oid: client_oid, context: crate::order_watcher::OrderContext::Entry };
            if !send_cancellable(&order_watch_tx, spot_req, &shutdown).await { /* Handle error */ }
            if !send_cancellable(&order_watch_tx, fut_req, &shutdown).await { app_state.inner.executing_pairs.remove(&symbol); }
        },
        (Ok(spot_order), Err(e_fut)) => {
            error!("[{}] LEGGING RISK ON ENTRY (Futures Failed): {:?}. Delegating to compensator.", symbol, e_fut);
            let task = CompensationTask {
                symbol: symbol.clone(),
                original_order_id: spot_order.order_id,
                base_qty_to_compensate: rounded_futures_qty, // This is correct, we need to compensate the *other* leg's quantity
                original_direction: ArbitrageDirection::BuySpotSellFutures,
                leg_to_compensate: OrderType::Spot, // This is correct, we need to compensate the *successful* leg
                is_entry: true,
            };
            if !send_cancellable(&compensation_tx, task, &shutdown).await { /* Handle error */ }
            app_state.inner.executing_pairs.remove(&symbol);
        },
        (Err(e_spot), Ok(futures_order)) => {
            error!("[{}] LEGGING RISK ON ENTRY (Spot Failed): {:?}. Delegating to compensator.", symbol, e_spot);
            let task = CompensationTask {
                symbol: symbol.clone(),
                original_order_id: futures_order.order_id,
                base_qty_to_compensate: rounded_futures_qty,
                original_direction: ArbitrageDirection::BuySpotSellFutures,
                leg_to_compensate: OrderType::Futures, // This is correct, we need to compensate the *successful* leg
                is_entry: true,
            };
            if !send_cancellable(&compensation_tx, task, &shutdown).await { /* Handle error */ }
            app_state.inner.executing_pairs.remove(&symbol);
        },
        (Err(e_spot), Err(e_fut)) => {
             error!("[{}] FAILED to place both entry orders. Spot: {:?}, Futures: {:?}. Releasing lock.", symbol, e_spot, e_fut);
             app_state.inner.executing_pairs.remove(&symbol);
        },
    }
}