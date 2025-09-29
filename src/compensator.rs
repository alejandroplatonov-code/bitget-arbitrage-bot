// src/compensator.rs

use crate::api_client::{ApiClient, PlaceFuturesOrderRequest, PlaceOrderRequest};
use crate::state::AppState;
use crate::types::{ArbitrageDirection, CompensationTask};
use crate::order_watcher::OrderType;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::sync::{mpsc, Notify};
use tracing::{error, info, warn};

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

        let result: Result<_, _> = match (task.leg_to_compensate, task.original_direction) {
            // Original was BuySpot, so we need to SellSpot.
            (OrderType::Spot, ArbitrageDirection::BuySpotSellFutures) => {
                let spot_quantity_scale = rules.spot_quantity_scale.unwrap_or(2);
                let qty = task.base_qty_to_compensate.trunc_with_scale(spot_quantity_scale);
                let req = PlaceOrderRequest { symbol: task.symbol.clone(), side: "sell".to_string(), order_type: "market".to_string(), force: "gtc".to_string(), size: qty.to_string(), client_oid: None };
                api_client.place_spot_order(req).await.map(|_| ())
            },
            // Original was SellFutures, so we need to BuyFutures.
            (OrderType::Futures, ArbitrageDirection::BuySpotSellFutures) => {
                let futures_quantity_scale = rules.futures_quantity_scale.unwrap_or(4); // Фолбэк на 4
                let qty = task.base_qty_to_compensate.trunc_with_scale(futures_quantity_scale);
                let req = PlaceFuturesOrderRequest { symbol: task.symbol.clone(), product_type: "USDT-FUTURES".to_string(), margin_mode: "isolated".to_string(), margin_coin: "USDT".to_string(), size: qty.to_string(), side: "buy".to_string(), trade_side: None, order_type: "market".to_string(), client_oid: None };
                api_client.place_futures_order(req).await.map(|_| ())
            },
            _ => Err(crate::error::AppError::LogicError("Unsupported compensation scenario".to_string())),
        };

        match result {
            Ok(_) => {
                info!("[Compensator] Successfully placed compensation order for {} (original order ID: {}). Task complete.", task.symbol, task.original_order_id);
                return; // Exit the loop on success.
            }
            Err(e) => {
                error!("[Compensator] Failed to place compensation order for {} (original order ID: {}): {:?}. Retrying in 5 seconds...", task.symbol, task.original_order_id, e);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}