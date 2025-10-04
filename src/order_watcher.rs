// src/order_watcher.rs

use crate::{state::AppState, types::MarketSnapshot};
use crate::types::TradeAnalysisLog;
use crate::api_client::ApiClient;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use crate::utils::send_cancellable;
use chrono::Local;
use std::str::FromStr;
use std::fs::OpenOptions;
use std::io::Write;
use tokio::task::JoinSet;
use std::sync::{Arc};
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use tracing::{error, info, warn};

/// Контекст ордера: для входа в позицию или для выхода.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum OrderContext {
    Entry,
    Exit,
}

/// Типы ордеров, которые мы можем отслеживать.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum OrderType {
    Spot,
    Futures,
}

/// Сообщение, которое `algorithm` отправляет в `order_watcher`.
#[derive(Debug, Clone)]
pub struct WatchOrderRequest {
    pub symbol: String,
    pub order_id: String,
    pub order_type: OrderType,
    pub client_oid: String,
    pub context: OrderContext,
}

/// Событие, которое `order_watcher` публикует в Redis при исполнении ордера.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OrderFilledEvent {
    pub symbol: String,
    pub order_id: String,
    pub order_type: OrderType,
    pub avg_price: String,
    pub base_volume: String,
    pub quote_volume: Option<String>, // Только для спота
    pub client_oid: String,
    pub context: OrderContext,
}

/// Запускает главный цикл "смотрителя ордеров".
/// Он принимает запросы на отслеживание и для каждого запускает отдельную задачу-трекер.
pub async fn run_order_watcher(
    mut order_rx: mpsc::Receiver<WatchOrderRequest>,
    api_client: Arc<ApiClient>,
    order_filled_tx: mpsc::Sender<OrderFilledEvent>, // <-- ИЗМЕНЕНИЕ: Принимаем MPSC Sender
    _app_state: Arc<AppState>,
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
                let client_clone = api_client.clone();
                let tx_clone = order_filled_tx.clone();
                let shutdown_clone = shutdown.clone();
                let app_state_clone = _app_state.clone();
                tracking_tasks.spawn(async move {
                    track_order(request, client_clone, tx_clone, shutdown_clone, app_state_clone).await;
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

/// Отслеживает один конкретный ордер до его исполнения или отмены.
async fn track_order(
    req: WatchOrderRequest,
    api_client: Arc<ApiClient>,
    order_filled_tx: mpsc::Sender<OrderFilledEvent>, // <-- ИЗМЕНЕНИЕ: Принимаем MPSC Sender
    shutdown: Arc<tokio::sync::Notify>, // <-- Добавлен аргумент
    app_state: Arc<AppState>,
) {
    let mut poll_interval = interval(Duration::from_millis(500)); // Опрашиваем каждые 500 мс

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

                if let Ok(order_info) = check_result {
                    if order_info.status == "filled" {
                        let execution_time = chrono::Utc::now().timestamp_millis();

                        info!("[OrderTracker] Order {} for {} FILLED. (clientOid: {})", &req.order_id, &req.symbol, &req.client_oid);
                        
                        let filled_event = OrderFilledEvent {
                            symbol: req.symbol.clone(),
                            order_id: req.order_id.clone(),
                            order_type: req.order_type,
                            avg_price: order_info.price_avg.clone(),
                            base_volume: order_info.base_volume.clone(),
                            quote_volume: order_info.quote_volume.clone(),
                            client_oid: req.client_oid.clone(),
                            context: req.context,
                        };
                        
                        // --- Шаг 1: Запись данных T4 ---
                        // Мы делаем это только для ордеров на вход, так как "черный ящик" создается только для них.
                        if req.context == OrderContext::Entry { // Убеждаемся, что это ордер на вход
                            // --- НАЧАЛО НОВОГО БЛОКА: ДЕЛАЕМ СНИМОК T4 ---
                            let mut snapshot_t4 = MarketSnapshot::default();
                            if let Some(pair_data) = app_state.inner.market_data.get(&req.symbol) {
                                // Используем функцию из algorithm.rs для форматирования.
                                // Примечание: в идеале эта функция должна быть в utils.
                                let bids_str = crate::algorithm::format_levels_for_analysis(&pair_data.futures_book.bids, true, 5);
                                let asks_str = crate::algorithm::format_levels_for_analysis(&pair_data.spot_book.asks, false, 5);
                                snapshot_t4.timestamp = execution_time;
                                snapshot_t4.futures_bids = bids_str;
                                snapshot_t4.spot_asks = asks_str;
                            }

                            if let Some(log_entry) = app_state.inner.trade_analysis_logs.get(&req.client_oid) {
                                let execution_details = match req.order_type {
                                    OrderType::Spot => {
                                        // Для спота используем quote_volume, это и есть стоимость (Cost).
                                        let cost_str = order_info.quote_volume.as_deref().unwrap_or("0");
                                        format!(
                                            "Filled: {} @ {} | Cost: {}",
                                            order_info.base_volume, order_info.price_avg, cost_str
                                        )
                                    },
                                    OrderType::Futures => {
                                        // Для фьючерсов ВЫЧИСЛЯЕМ выручку (Revenue), т.к. quote_volume ненадежен.
                                        let price = Decimal::from_str(&order_info.price_avg).unwrap_or_default();
                                        let qty = Decimal::from_str(&order_info.base_volume).unwrap_or_default();
                                        let revenue = price * qty;
                                        format!(
                                            "Filled: {} @ {} | Revenue: {:.4}",
                                            order_info.base_volume, order_info.price_avg, revenue
                                        )
                                    }
                                };
                                // Записываем детали исполнения в лог
                                log_entry.execution_logs.insert(req.order_id.clone(), (execution_time, execution_details.clone(), snapshot_t4));

                                // --- Шаг 2: Проверка на завершение ---
                                if log_entry.execution_logs.len() >= 2 { // Если оба ордера (спот и фьючерс) исполнены
                                    info!("[Analysis] Both legs filled for clientOid {}. Generating analysis report.", req.client_oid);
                                    // --- Шаг 3: Генерация отчета ---
                                    generate_final_report(log_entry.value());
                                    // Удаляем запись, чтобы очистить память
                                    app_state.inner.trade_analysis_logs.remove(&req.client_oid);
                                }
                            }
                        }

                        // --- Шаг 4: Отправка события ---
                        // Отправляем событие в PositionManager в любом случае, чтобы он мог обновить состояние позиции.
                        if !send_cancellable(&order_filled_tx, filled_event, &shutdown).await {
                            error!("[OrderWatcher] Failed to send OrderFilledEvent for order {} to PositionManager (channel closed or shutdown).", &req.order_id);
                        }
                        
                        break;
                    }
                }
            }
        }
    }
}

/// Формирует и записывает в файл `logs/analysis.log` финальный отчет по сделке.
fn generate_final_report(log: &TradeAnalysisLog) {
    let mut report = String::new();

    let t1_dt = chrono::DateTime::from_timestamp_millis(log.snapshot_at_decision.timestamp).unwrap().with_timezone(&Local);
    let t2_dt = chrono::DateTime::from_timestamp_millis(log.snapshot_before_send.timestamp).unwrap().with_timezone(&Local);
    let t3_dt = chrono::DateTime::from_timestamp_millis(log.snapshot_at_acceptance.timestamp).unwrap().with_timezone(&Local);

    let t2_delay = log.snapshot_before_send.timestamp - log.snapshot_at_decision.timestamp;
    let t3_delay = log.snapshot_at_acceptance.timestamp - log.snapshot_at_decision.timestamp;

    report.push_str(&format!(
        "\n--- TRADE ANALYSIS [{}] clientOid: {} ---\n",
        log.symbol, log.client_oid
    ));

    report.push_str(&format!(
        "[T1: DECISION @ {}]\n",
        t1_dt.format("%H:%M:%S%.3f")
    ));
    report.push_str(&format!("  ├─ Simulation: {}\n", log.simulation_log));
    report.push_str("  └─ Market State:\n");
    report.push_str(&format!("      ├─ Futures Bids: {}\n", log.snapshot_at_decision.futures_bids));
    report.push_str(&format!("      └─ Spot Asks:    {}\n\n", log.snapshot_at_decision.spot_asks));

    report.push_str(&format!(
        "[T2: BEFORE SEND @ {} (+{}ms)]\n",
        t2_dt.format("%H:%M:%S%.3f"), t2_delay
    ));
    report.push_str("  └─ Market State:\n");
    report.push_str(&format!("      ├─ Futures Bids: {}\n", log.snapshot_before_send.futures_bids));
    report.push_str(&format!("      └─ Spot Asks:    {}\n\n", log.snapshot_before_send.spot_asks));

    report.push_str(&format!(
        "[T3: ACCEPTED by Exchange @ {} (+{}ms)]\n",
        t3_dt.format("%H:%M:%S%.3f"), t3_delay
    ));
    report.push_str("  └─ Market State:\n");
    report.push_str(&format!("      ├─ Futures Bids: {}\n", log.snapshot_at_acceptance.futures_bids));
    report.push_str(&format!("      └─ Spot Asks:    {}\n\n", log.snapshot_at_acceptance.spot_asks));

    report.push_str("[T4: EXECUTION]\n");

    let mut spot_cost = Decimal::ZERO;
    let mut futures_revenue = Decimal::ZERO;

    for leg in log.execution_logs.iter() {
        let order_id = leg.key();
        let (exec_time, details, snapshot_t4) = leg.value();
        let exec_dt = chrono::DateTime::from_timestamp_millis(*exec_time).unwrap().with_timezone(&Local); // No change needed here
        let exec_delay = *exec_time - log.snapshot_at_acceptance.timestamp;

        // Пытаемся определить тип лега по деталям
        let leg_type = if details.contains("Cost") { "SPOT" } else if details.contains("Revenue") { "FUTURES" } else { "UNKNOWN" };

        report.push_str(&format!(
            "  ├─ {} Leg (orderId: {}...):\n",
            leg_type,
            &order_id[..8]
        ));
        report.push_str(&format!(
            "  │   ├─ Executed @ {} (Delay: {}ms) | {}\n",
            exec_dt.format("%H:%M:%S%.3f"),
            exec_delay,
            details
        ));
        report.push_str("  │   └─ Market State @ Execution:\n");
        report.push_str(&format!("  │       ├─ Futures Bids: {}\n", snapshot_t4.futures_bids));
        report.push_str(&format!("  │       └─ Spot Asks:    {}\n", snapshot_t4.spot_asks));


        // Для финального анализа
        if leg_type == "SPOT" {
            if let Some(cost_str) = details.split("Cost: ").last().and_then(|s| s.split_whitespace().next()) {
                spot_cost = Decimal::from_str(cost_str.trim()).unwrap_or_default();
            }
        } else if leg_type == "FUTURES" {
            if let Some(rev_str) = details.split("Revenue: ").last().and_then(|s| s.split_whitespace().next()) {
                futures_revenue = Decimal::from_str(rev_str.trim()).unwrap_or_default();
            }
        }
    }

    // --- ANALYSIS SECTION ---
    let mut sim_fut_rev = Decimal::ZERO;
    let mut sim_spot_cost = Decimal::ZERO;
    for part in log.simulation_log.split(" | ") {
        if let Some(val_str) = part.strip_prefix("R_fut: ") {
            sim_fut_rev = Decimal::from_str(val_str).unwrap_or_default();
        }
        if let Some(val_str) = part.strip_prefix("C_spot: ") {
            sim_spot_cost = Decimal::from_str(val_str).unwrap_or_default();
        }
    }

    let sim_pnl = sim_fut_rev - sim_spot_cost;
    let actual_pnl = futures_revenue - spot_cost;
    let slippage = actual_pnl - sim_pnl;

    report.push_str("\n--- ANALYSIS ---\n");
    report.push_str(&format!("- Simulated PnL: {:.4} USDT (Revenue: {:.4} - Cost: {:.4})\n", sim_pnl, sim_fut_rev, sim_spot_cost));
    report.push_str(&format!("- Actual PnL:    {:.4} USDT (Revenue: {:.4} - Cost: {:.4})\n", actual_pnl, futures_revenue, spot_cost));
    report.push_str(&format!("- Slippage:      {:.4} USDT\n", slippage));
    if slippage < Decimal::ZERO {
        report.push_str("- Diagnosis: Negative slippage indicates that the market moved against the trade between decision and execution.\n");
    }
    report.push_str("\n--- END OF REPORT ---\n");

    // Запись в файл
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("logs/analysis.log") {
        if let Err(e) = writeln!(file, "{}", report) {
            error!("[Analysis] Failed to write to analysis.log: {}", e);
        }
    } else {
        error!("[Analysis] Failed to open or create logs/analysis.log");
    }
}