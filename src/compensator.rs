// src/compensator.rs

use crate::api_client::{ApiClient, PlaceFuturesOrderRequest, PlaceOrderRequest};
use crate::state::AppState;
use rust_decimal::prelude::FromPrimitive;
use crate::types::{CompensationTask};
use crate::order_watcher::OrderType;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::sync::{mpsc, Notify};
use tracing::{error, info, warn};

const DUST_THRESHOLD_FACTOR: f64 = 0.9; // Считаем, что баланс/позиция отсутствует, если она меньше 90% от ожидаемой

/// Runs the compensator service, which handles closing orphaned trade legs.
/// It receives tasks and retries them indefinitely until they succeed.
pub async fn run_compensator(
    mut task_rx: mpsc::Receiver<CompensationTask>,
    api_client: Arc<ApiClient>,
    app_state: Arc<AppState>,
    shutdown: Arc<Notify>,
) {
    info!("[Compensator] Service started. Waiting for compensation tasks...");

    // --- ИЗМЕНЕНИЕ 1: Создаем JoinSet для управления задачами компенсации ---
    let mut compensation_tasks = JoinSet::new();

    loop {
        tokio::select! {
            // Ветка 1: Получаем новую задачу для выполнения
            Some(task) = task_rx.recv() => {
                info!("[Compensator] Received new compensation task for {}: {:?}", task.symbol, task);
                
                // --- ИЗМЕНЕНИЕ 2: Запускаем задачу внутри управляемого JoinSet ---
                compensation_tasks.spawn({
                    let client = api_client.clone();
                    let state = app_state.clone();
                    async move {
                        handle_compensation_task(task, client, state).await;
                    }
                });
            },

            // Ветка 2: Периодически очищаем JoinSet от успешно завершившихся задач
            Some(_) = compensation_tasks.join_next(), if !compensation_tasks.is_empty() => {},

            // Ветка 3: Получаем сигнал о завершении работы
            _ = shutdown.notified() => {
                info!("[Compensator] Shutdown signal received. Aborting all active compensation tasks.");
                
                // --- ИЗМЕНЕНИЕ 3 (КЛЮЧЕВОЕ): Принудительно завершаем все дочерние задачи ---
                compensation_tasks.abort_all();

                break; // Выходим из главного цикла
            }
        }
    }
    warn!("[Compensator] Service is shutting down.");
}

/// Handles a single compensation task, retrying until the compensating order is placed.
async fn handle_compensation_task(
    task: CompensationTask,
    api_client: Arc<ApiClient>,
    app_state: Arc<AppState>,
) {
    let mut attempts = 0;
    loop {
        attempts += 1;
        warn!("[Compensator] Attempt #{} to compensate leg for {} (original order ID: {})", attempts, task.symbol, task.original_order_id);

        // --- ИСПРАВЛЕНИЕ: Получаем правила округления из AppState ---
        let rules = app_state.inner.symbol_rules.get(&task.symbol)
            .map(|r| *r.value())
            .unwrap_or_default();

        let result = match task.leg_to_compensate {
            // Спотовый ордер прошел, а фьючерсный - нет. Нужно компенсировать спот.
            OrderType::Spot => {
                let base_coin = task.symbol.replace("USDT", "");
                match api_client.get_spot_balance(&base_coin).await {
                    Ok(actual_balance) => {
                        if actual_balance < task.base_qty_to_compensate * rust_decimal::Decimal::from_f64(DUST_THRESHOLD_FACTOR).unwrap() {
                            warn!("[Compensator] CANCELLING task for {}. Spot balance ({}) is too low. Assumed already compensated.", task.symbol, actual_balance);
                            return; // Выходим из функции, задача отменяется
                        }

                        let side = if task.is_entry { "sell" } else { "buy" };
                        info!("[Compensator] Compensating SPOT leg for {}. Actual balance: {}. Placing {} order.", task.symbol, actual_balance, side.to_uppercase());
                        let spot_scale = rules.spot_quantity_scale.unwrap_or(2);
                        let sell_qty = actual_balance.trunc_with_scale(spot_scale);

                        let req = PlaceOrderRequest { symbol: task.symbol.clone(), side: side.to_string(), order_type: "market".to_string(), force: "gtc".to_string(), size: sell_qty.to_string(), client_oid: None };
                        api_client.place_spot_order(req).await.map(|_| ()).map_err(|e| e.into())
                    }
                    Err(e) => {
                        error!("[Compensator] Could not get SPOT balance for {}: {:?}. Cannot verify compensation safety. Retrying...", task.symbol, e);
                        Err(e)
                    }
                }
            },
            // Фьючерсный ордер прошел, а спотовый - нет. Нужно компенсировать фьючерс.
            OrderType::Futures => {
                match api_client.get_futures_position(&task.symbol).await {
                    Ok(actual_position_size) => {
                        if actual_position_size < task.base_qty_to_compensate * rust_decimal::Decimal::from_f64(DUST_THRESHOLD_FACTOR).unwrap() {
                            warn!("[Compensator] CANCELLING task for {}. Futures position ({}) is too small. Assumed already compensated.", task.symbol, actual_position_size);
                            return; // Выходим из функции, задача отменяется
                        }

                        let side = if task.is_entry { "buy" } else { "sell" };
                        info!("[Compensator] Compensating FUTURES leg for {}. Actual position size: {}. Placing {} order to close.", task.symbol, actual_position_size, side.to_uppercase());
                        let req = PlaceFuturesOrderRequest {
                            symbol: task.symbol.clone(),
                            product_type: "USDT-FUTURES".to_string(),
                            margin_mode: "isolated".to_string(),
                            margin_coin: "USDT".to_string(),
                            size: actual_position_size.to_string(), // Закрываем фактический размер
                            side: side.to_string(),
                            trade_side: Some("close".to_string()),
                            order_type: "market".to_string(),
                            client_oid: None,
                            price: None,
                        };
                        api_client.place_futures_order(req).await.map(|_| ()).map_err(|e| e.into())
                    }
                    Err(e) => {
                        error!("[Compensator] Could not get FUTURES position for {}: {:?}. Cannot verify compensation safety. Retrying...", task.symbol, e);
                        Err(e)
                    }
                }
            }
        };

        match result {
            Ok(_) => {
                info!("[Compensator] Successfully placed compensation order for {} (original order ID: {}). Task complete.", task.symbol, task.original_order_id);
                return; // Выходим из цикла и завершаем задачу
            }
            Err(e) => {
                error!("[Compensator] Failed to place compensation order for {} (original order ID: {}): {:?}. Retrying in 5 seconds...", task.symbol, task.original_order_id, e);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}