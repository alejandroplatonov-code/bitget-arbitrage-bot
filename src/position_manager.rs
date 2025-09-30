// src/position_manager.rs

use crate::api_client::ApiClient;
use crate::config::Config;
use crate::order_watcher::{OrderContext, OrderFilledEvent, OrderType};
use crate::state::AppState;
use crate::types::{ActivePosition, ArbitrageDirection, CompletedTrade, BalanceCacheState};
use dashmap::DashMap;
use rust_decimal::{dec, Decimal, prelude::FromStr};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// Основная функция, запускающая менеджер позиций.
/// Он слушает события об исполнении ордеров и обновляет состояние позиций.
pub async fn run_position_manager(
    app_state: Arc<AppState>,
    api_client: Arc<ApiClient>,
    config: Arc<Config>,
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
                        handle_entry_fill(filled_event, &app_state, &pending_entry_orders, &api_client, &config).await;
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
    api_client: &Arc<ApiClient>,
    _config: &Arc<Config>, // config is now available if needed
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
            balance_cache: Default::default(),
        };

        // --- НОВАЯ ЛОГИКА: Запускаем фоновую задачу для кэширования баланса ---
        tokio::spawn({
            let client = api_client.clone();
            let position_clone = new_position.clone();
            let symbol = new_position.symbol.clone();
            let app_state = app_state.clone(); // Клонируем AppState для доступа к ценам

            async move {
                tokio::time::sleep(Duration::from_millis(500)).await;
                let base_coin = symbol.replace("USDT", "");
                const DUST_THRESHOLD_USDT: Decimal = dec!(4);
                const MAX_ATTEMPTS: usize = 10;

                let mut attempts = 0;
                loop {
                    if attempts >= MAX_ATTEMPTS {
                        error!("[BalanceCacher] CRITICAL: Could not cache a valid (non-dust) balance for {} after {} attempts. It may not close correctly.", symbol, MAX_ATTEMPTS);
                        error!("[BalanceCacher] CRITICAL: Could not cache balance for {}. Setting cache state to Failed.", symbol);
                        *position_clone.balance_cache.lock().unwrap() = BalanceCacheState::Failed; // <-- Устанавливаем статус "Сбой"
                        break;
                    }
                    attempts += 1;

                    match client.get_spot_balance(&base_coin).await {
                        Ok(balance) => {
                            // Проверяем, не является ли баланс "пылью"
                            let spot_price = app_state.inner.market_data.get(&symbol)
                                .and_then(|p| p.value().spot_last_price)
                                .unwrap_or(Decimal::ONE); // Если цены нет, считаем 1:1 для безопасности

                            let balance_in_usdt = balance * spot_price;

                            if balance_in_usdt >= DUST_THRESHOLD_USDT {
                                info!("[BalanceCacher] Successfully cached valid balance for {}: {}", symbol, balance);
                                *position_clone.balance_cache.lock().unwrap() = BalanceCacheState::Cached(balance); // <-- Устанавливаем статус "Готово"
                                break; // Успех, баланс валидный, выходим.
                            } else {
                                warn!("[BalanceCacher] Attempt #{}: Fetched balance for {} is considered dust: {} (Value: {:.2} USDT). Retrying...", attempts, symbol, balance, balance_in_usdt);
                                // Не выходим, продолжаем цикл
                            }
                        },
                        Err(e) => {
                            warn!("[BalanceCacher] Attempt #{}: API error fetching balance for {}: {:?}. Retrying...", attempts, symbol, e);
                            // Не выходим, продолжаем цикл
                        }
                    }
                    // Пауза перед следующей попыткой (и в случае пыли, и в случае ошибки API)
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        });
        
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