// src/position_manager.rs

use crate::order_watcher::{OrderContext, OrderFilledEvent, OrderType};
use crate::state::AppState;
use crate::types::{ActivePosition, ArbitrageDirection, CompletedTrade, PositionState};
use dashmap::DashMap;
use rust_decimal::{Decimal, prelude::FromStr};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, warn};

/// Основная функция, запускающая менеджер позиций.
/// Он слушает события об исполнении ордеров и обновляет состояние позиций.
pub async fn run_position_manager(
    app_state: Arc<AppState>,
    mut event_rx: mpsc::Receiver<OrderFilledEvent>,
    shutdown: Arc<tokio::sync::Notify>,
) {
    info!("[PositionManager] Service started.");

    // Хранилища для сопоставления парных ордеров по client_oid
    let pending_entry_orders = Arc::new(DashMap::<String, (Option<OrderFilledEvent>, Option<OrderFilledEvent>)>::new());
    let pending_exit_orders = Arc::new(DashMap::<String, (Option<OrderFilledEvent>, Option<OrderFilledEvent>)>::new());

    info!("[PositionManager] Waiting for OrderFilledEvent messages from the dispatcher.");

    loop {
        tokio::select! {
            Some(filled_event) = event_rx.recv() => {
                info!("[PositionManager] Received OrderFilledEvent for {} (context: {:?})", filled_event.symbol, filled_event.context);

                match filled_event.context {
                    OrderContext::Entry => {
                        handle_entry_fill(filled_event, &app_state, &pending_entry_orders).await;
                    },
                    OrderContext::Exit => {
                        handle_exit_fill(filled_event, &app_state, &pending_exit_orders).await;
                    }
                }
            },
            _ = shutdown.notified() => {
                info!("[PositionManager] Shutdown signal received. Exiting.");
                break;
            }
        }
    }
    warn!("[PositionManager] MPSC channel closed or shutdown signal received. Service is shutting down.");
}

/// Обрабатывает исполнение ОДНОГО ордера на ВХОД.
async fn handle_entry_fill(
    event: OrderFilledEvent,
    app_state: &Arc<AppState>,
    pending_orders: &DashMap<String, (Option<OrderFilledEvent>, Option<OrderFilledEvent>)>,
) {
    let mut entry = pending_orders.entry(event.client_oid.clone()).or_default();
    match event.order_type {
        OrderType::Spot => entry.0 = Some(event),
        OrderType::Futures => entry.1 = Some(event),
    }

    if let (Some(spot_fill), Some(futures_fill)) = (&entry.0, &entry.1) {
        info!("[PositionManager] Matched both ENTRY fills for clientOid {}. Opening position.", &spot_fill.client_oid);

        // Снимаем блокировку с пары, так как ордера исполнились и позиция создается.
        app_state.inner.executing_pairs.remove(&spot_fill.symbol);

        let cost_spot_entry = spot_fill.quote_volume.as_ref().and_then(|s| Decimal::from_str(s).ok()).unwrap_or_default();
        let futures_price = Decimal::from_str(&futures_fill.avg_price).unwrap_or_default();
        let futures_qty = Decimal::from_str(&futures_fill.base_volume).unwrap_or_default();
        let revenue_futures_entry = futures_price * futures_qty;

        let entry_spread_percent = if !cost_spot_entry.is_zero() {
            ((revenue_futures_entry - cost_spot_entry) / cost_spot_entry) * Decimal::from(100)
        } else {
            Decimal::ZERO
        };

        let new_position = ActivePosition {
            symbol: spot_fill.symbol.clone(),
            direction: ArbitrageDirection::BuySpotSellFutures,
            futures_base_qty: Decimal::from_str(&futures_fill.base_volume).unwrap_or_default(),
            spot_base_qty: Decimal::from_str(&spot_fill.base_volume).unwrap_or_default(),
            entry_time: chrono::Utc::now().timestamp_millis(),
            cost_spot_entry,
            revenue_futures_entry,
            spot_entry_vwap: Decimal::from_str(&spot_fill.avg_price).unwrap_or_default(),
            futures_entry_vwap: futures_price,
            current_state: Default::default(),
            entry_spread_percent,
        };

        // Сохраняем в локальном состоянии
        app_state.inner.active_positions.insert(spot_fill.symbol.clone(), new_position);

        // Удаляем из временного хранилища
        pending_orders.remove(&spot_fill.client_oid);
    }
}

/// Обрабатывает исполнение ОДНОГО ордера на ВЫХОД.
async fn handle_exit_fill(
    event: OrderFilledEvent,
    app_state: &Arc<AppState>,
    pending_orders: &DashMap<String, (Option<OrderFilledEvent>, Option<OrderFilledEvent>)>,
) {
    let mut entry = pending_orders.entry(event.client_oid.clone()).or_default();
    match event.order_type {
        OrderType::Spot => entry.0 = Some(event),
        OrderType::Futures => entry.1 = Some(event),
    }

    if let (Some(spot_fill), Some(futures_fill)) = (&entry.0, &entry.1) {
        info!("[PositionManager] Matched both EXIT fills for clientOid {}. Closing position.", &spot_fill.client_oid);

        if let Some((_, active_pos)) = app_state.inner.active_positions.remove(&spot_fill.symbol) {
            let revenue_spot_exit = spot_fill.quote_volume.as_ref().and_then(|s| Decimal::from_str(s).ok()).unwrap_or_default();
            let cost_futures_exit = Decimal::from_str(&futures_fill.avg_price).unwrap_or_default() * Decimal::from_str(&futures_fill.base_volume).unwrap_or_default();

            let final_pnl = (active_pos.revenue_futures_entry + revenue_spot_exit) - (active_pos.cost_spot_entry + cost_futures_exit);

            let exit_spread_percent = if !cost_futures_exit.is_zero() {
                ((revenue_spot_exit - cost_futures_exit) / cost_futures_exit) * Decimal::from(100)
            } else {
                Decimal::ZERO
            };

            let completed_trade = CompletedTrade {
                entry_data: active_pos.clone(),
                exit_time: chrono::Utc::now().timestamp_millis(),
                cost_exit: cost_futures_exit,
                revenue_exit: revenue_spot_exit,
                final_pnl,
                entry_spread_percent: active_pos.entry_spread_percent,
                exit_spread_percent,
            };

            // Сохраняем в истории
            app_state.inner.completed_trades.entry(spot_fill.symbol.clone()).or_default().push(completed_trade);
            info!("[PositionManager] Position for {} finalized. PnL: {:.4}", &spot_fill.symbol, final_pnl);
        } else {
            warn!("[PositionManager] Received exit fills for {}, but no active position was found in the state.", &spot_fill.symbol);
        }

        // Удаляем из временного хранилища
        pending_orders.remove(&spot_fill.client_oid);

        // Снимаем блокировку с пары, так как позиция полностью закрыта.
        app_state.inner.executing_pairs.remove(&spot_fill.symbol);
    }
}