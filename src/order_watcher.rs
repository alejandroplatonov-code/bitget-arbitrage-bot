// src/order_watcher.rs

use crate::api_client::{ApiClient, PlaceOrderRequest};
use crate::state::AppState;
use crate::types::MarketSnapshot;
use crate::utils::send_cancellable;
use chrono::{Local, TimeZone, Utc};
use rust_decimal::{dec, Decimal};
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::Write;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::{interval, Duration};
use tracing::{error, info, warn};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
pub enum OrderContext {
    #[default]
    Entry,
    Exit,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
pub enum OrderType {
    #[default]
    Spot,
    Futures,
}

#[derive(Debug, Clone, Default)]
pub struct WatchOrderRequest {
    pub symbol: String,
    pub order_id: String,
    pub order_type: OrderType,
    pub client_oid: String,
    pub is_maker: bool,
    pub context: OrderContext,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OrderFilledEvent {
    pub symbol: String,
    pub order_id: String,
    pub order_type: OrderType,
    pub avg_price: String,
    pub base_volume: String,
    pub quote_volume: Option<String>,
    pub client_oid: String,
    pub context: OrderContext,
}

pub async fn run_order_watcher(
    mut order_rx: mpsc::Receiver<WatchOrderRequest>,
    api_client: Arc<ApiClient>,
    order_filled_tx: mpsc::Sender<OrderFilledEvent>,
    app_state: Arc<AppState>,
    shutdown: Arc<tokio::sync::Notify>,
) {
    info!("[OrderWatcher] Service started.");
    let mut tracking_tasks = JoinSet::new();

    loop {
        tokio::select! {
            biased;
            _ = shutdown.notified() => {
                info!("[OrderWatcher] Shutdown signal received. Aborting all active order trackers.");
                tracking_tasks.abort_all();
                break;
            },
            Some(request) = order_rx.recv() => {
                info!("[OrderWatcher] Received request to watch order ID: {} (clientOid: {})", request.order_id, request.client_oid);
                tracking_tasks.spawn({
                    let client = api_client.clone();
                    let filled_tx = order_filled_tx.clone();
                    let shutdown = shutdown.clone();
                    let app_state = app_state.clone();
                    async move {
                        track_order(request, client, filled_tx, shutdown, app_state).await;
                    }
                });
            },
            Some(res) = tracking_tasks.join_next(), if !tracking_tasks.is_empty() => {
                if let Err(e) = res {
                    if !e.is_cancelled() {
                        error!("[OrderWatcher] A tracking task failed: {:?}", e);
                    }
                }
            }
        }
    }
    warn!("[OrderWatcher] Service is shutting down.");
}

async fn track_order(
    req: WatchOrderRequest,
    api_client: Arc<ApiClient>,
    order_filled_tx: mpsc::Sender<OrderFilledEvent>,
    shutdown: Arc<tokio::sync::Notify>,
    app_state: Arc<AppState>,
) {
    let mut poll_interval = interval(Duration::from_millis(500));

    loop {
        tokio::select! {
            biased;
            _ = shutdown.notified() => {
                info!("[OrderTracker] Shutdown signal received for order {}. Stopping tracking.", req.order_id);
                break;
            },
            _ = poll_interval.tick() => {
                let check_result = match req.order_type {
                    OrderType::Spot => api_client.get_spot_order(&req.order_id).await,
                    OrderType::Futures => api_client.get_futures_order(&req.symbol, &req.order_id).await,
                };

                match check_result {
                    Ok(order_info) => {
                        if order_info.status == "filled" || order_info.status == "partially_filled" {
                            info!("[OrderTracker] Order {} for {} {}. (clientOid: {})", &req.order_id, &req.symbol, order_info.status.to_uppercase(), &req.client_oid);

                            // --- НОВАЯ ЛОГИКА MAKER-TAKER ---
                            if req.is_maker && req.context == OrderContext::Entry {
                                // Это наш "якорный" Maker ордер исполнился!
                                // Теперь нужно немедленно отправить Taker ордер на спот.
                                let base_volume = Decimal::from_str(&order_info.base_volume).unwrap_or_default();
                                if !base_volume.is_zero() {
                                    info!("[MakerTaker] Maker leg filled for {}. Executing Taker leg on SPOT.", req.symbol);
                                    let taker_req = PlaceOrderRequest {
                                        symbol: req.symbol.clone(),
                                        side: "buy".to_string(),
                                        order_type: "market".to_string(),
                                        force: "gtc".to_string(),
                                        size: (base_volume * Decimal::from_str(&order_info.price_avg).unwrap_or_default() * dec!(1.01)).to_string(), // Покупаем на 1% больше USDT, чтобы покрыть slippage
                                        client_oid: Some(req.client_oid.clone()),
                                    };

                                    match api_client.place_spot_order(taker_req).await {
                                        Ok(taker_order) => {
                                            info!("[MakerTaker] Taker SPOT order placed for {}: ID {}", req.symbol, taker_order.order_id);
                                            // Мы не отслеживаем Taker ордер отдельно, так как он рыночный и исполнится быстро.
                                            // PositionManager все равно получит событие FILLED по нему.
                                        },
                                        Err(e) => {
                                            error!("[MakerTaker] CRITICAL: Failed to place Taker SPOT order for {}: {:?}. MANUAL INTERVENTION REQUIRED!", req.symbol, e);
                                            // TODO: Отправить в компенсатор задачу на закрытие только что открытой фьючерсной позиции
                                        }
                                    }
                                }
                            }
                            
                            let filled_event = OrderFilledEvent {
                                symbol: req.symbol.clone(), order_id: req.order_id.clone(), order_type: req.order_type,
                                avg_price: order_info.price_avg.clone(), base_volume: order_info.base_volume.clone(),
                                quote_volume: order_info.quote_volume.clone(), client_oid: req.client_oid.clone(), context: req.context,
                            };

                            process_filled_order(&req, &order_info, &app_state);

                            if !send_cancellable(&order_filled_tx, filled_event, &shutdown).await {
                                error!("[OrderWatcher] Failed to send OrderFilledEvent for order {}", &req.order_id);
                            }

                            if order_info.status == "filled" {
                                break; // Выходим из цикла, если ордер исполнен полностью
                            }
                        } else if order_info.status == "cancelled" {
                            info!("[OrderTracker] Order {} for {} was CANCELLED. Stopping tracking.", &req.order_id, &req.symbol);
                            break;
                        }
                    },
                    Err(e) => {
                        warn!("[OrderTracker] Failed to get order info for {}: {:?}. It might have been cancelled. Stopping tracking.", req.order_id, e);
                        break;
                    }
                }
            }
        }
    }
}

fn process_filled_order(
    req: &WatchOrderRequest,
    order_info: &crate::api_client::OrderInfo,
    app_state: &Arc<AppState>,
) {
    if req.context != OrderContext::Entry { return; }

    let execution_time = Utc::now().timestamp_millis();
    let mut snapshot_t4 = MarketSnapshot::default();
    if let Some(pair_data) = app_state.inner.market_data.get(&req.symbol) {
        snapshot_t4 = MarketSnapshot {
            timestamp: execution_time,
            futures_bids: crate::algorithm::format_levels_for_analysis(&pair_data.futures_book.bids, true, 5),
            spot_asks: crate::algorithm::format_levels_for_analysis(&pair_data.spot_book.asks, false, 5),
        };
    }

    if let Some(log_entry) = app_state.inner.trade_analysis_logs.get(&req.client_oid) {
        let details = match req.order_type {
            OrderType::Spot => {
                let cost = order_info.quote_volume.as_deref().unwrap_or("0");
                format!("Filled: {} @ {} | Cost: {}", order_info.base_volume, order_info.price_avg, cost)
            },
            OrderType::Futures => {
                let price = Decimal::from_str(&order_info.price_avg).unwrap_or_default();
                let qty = Decimal::from_str(&order_info.base_volume).unwrap_or_default();
                format!("Filled: {} @ {} | Revenue: {:.4}", qty, price, price * qty)
            }
        };

        log_entry.execution_logs.insert(req.order_id.clone(), (execution_time, details, snapshot_t4));

        if log_entry.execution_logs.len() >= 2 {
            info!("[Analysis] Both legs filled for clientOid {}. Generating analysis report.", req.client_oid);
            generate_final_report(log_entry.value());
            app_state.inner.trade_analysis_logs.remove(&req.client_oid);
        }
    }
}

fn generate_final_report(log: &crate::types::TradeAnalysisLog) {
    let mut report = String::new();
    let t1_dt = Local.timestamp_millis_opt(log.snapshot_at_decision.timestamp).unwrap();
    let t2_dt = Local.timestamp_millis_opt(log.snapshot_before_send.timestamp).unwrap();
    let t3_dt = Local.timestamp_millis_opt(log.snapshot_at_acceptance.timestamp).unwrap();
    
    report.push_str(&format!("\n--- TRADE ANALYSIS [{}] clientOid: {} ---\n", log.symbol, log.client_oid));
    report.push_str(&format!("[T1: DECISION @ {}]\n", t1_dt.format("%H:%M:%S%.3f")));
    report.push_str(&format!("  ├─ Simulation: {}\n", log.simulation_log));
    report.push_str("  └─ Market State:\n");
    report.push_str(&format!("      ├─ Futures Bids: {}\n", log.snapshot_at_decision.futures_bids));
    report.push_str(&format!("      └─ Spot Asks:    {}\n\n", log.snapshot_at_decision.spot_asks));

    report.push_str(&format!("[T2: BEFORE SEND @ {} (+{}ms)]\n", t2_dt.format("%H:%M:%S%.3f"), (t2_dt - t1_dt).num_milliseconds()));
    report.push_str("  └─ Market State:\n");
    report.push_str(&format!("      ├─ Futures Bids: {}\n", log.snapshot_before_send.futures_bids));
    report.push_str(&format!("      └─ Spot Asks:    {}\n\n", log.snapshot_before_send.spot_asks));

    report.push_str(&format!("[T3: ACCEPTED by Exchange @ {} (+{}ms)]\n", t3_dt.format("%H:%M:%S%.3f"), (t3_dt - t1_dt).num_milliseconds()));
    report.push_str("  └─ Market State:\n");
    report.push_str(&format!("      ├─ Futures Bids: {}\n", log.snapshot_at_acceptance.futures_bids));
    report.push_str(&format!("      └─ Spot Asks:    {}\n\n", log.snapshot_at_acceptance.spot_asks));

    report.push_str("[T4: EXECUTION]\n");

    let mut spot_cost = Decimal::ZERO;
    let mut futures_revenue = Decimal::ZERO;

    for leg in log.execution_logs.iter() {
        let (exec_time, details, snapshot_t4) = leg.value();
        let exec_dt = Local.timestamp_millis_opt(*exec_time).unwrap();
        let leg_type = if details.contains("Cost") { "SPOT" } else { "FUTURES" };

        report.push_str(&format!("  ├─ {} Leg (orderId: {}...):\n", leg_type, &leg.key()[..8]));
        report.push_str(&format!("  │   ├─ Executed @ {} (Delay: {}ms) | {}\n", exec_dt.format("%H:%M:%S%.3f"), (*exec_time - log.snapshot_at_acceptance.timestamp), details));
        report.push_str("  │   └─ Market State @ Execution:\n");
        report.push_str(&format!("  │       ├─ Futures Bids: {}\n", snapshot_t4.futures_bids));
        report.push_str(&format!("  │       └─ Spot Asks:    {}\n", snapshot_t4.spot_asks));

        if leg_type == "SPOT" {
            if let Some(val) = details.split("Cost: ").last() { spot_cost = Decimal::from_str(val.trim()).unwrap_or_default(); }
        } else {
            if let Some(val) = details.split("Revenue: ").last() { futures_revenue = Decimal::from_str(val.trim()).unwrap_or_default(); }
        }
    }

    let mut sim_fut_rev = Decimal::ZERO;
    let mut sim_spot_cost = Decimal::ZERO;
    for part in log.simulation_log.split(" | ") {
        if let Some(val) = part.strip_prefix("R_fut: ") { sim_fut_rev = Decimal::from_str(val.trim()).unwrap_or_default(); }
        if let Some(val) = part.strip_prefix("C_spot: ") { sim_spot_cost = Decimal::from_str(val.trim()).unwrap_or_default(); }
    }

    let sim_pnl = sim_fut_rev - sim_spot_cost;
    let actual_pnl = futures_revenue - spot_cost;
    
    report.push_str("\n--- ANALYSIS ---\n");
    report.push_str(&format!("- Simulated PnL: {:.4} USDT (Revenue: {:.4} - Cost: {:.4})\n", sim_pnl, sim_fut_rev, sim_spot_cost));
    report.push_str(&format!("- Actual PnL:    {:.4} USDT (Revenue: {:.4} - Cost: {:.4})\n", actual_pnl, futures_revenue, spot_cost));
    report.push_str(&format!("- Slippage:      {:.4} USDT\n", actual_pnl - sim_pnl));
    report.push_str("- Diagnosis: Negative slippage indicates that the market moved against the trade between decision and execution.\n\n--- END OF REPORT ---\n");

    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("logs/analysis.log") {
        if let Err(e) = writeln!(file, "{}", report) {
            error!("[Analysis] Failed to write to analysis.log: {}", e);
        }
    }
}