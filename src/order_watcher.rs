// src/order_watcher.rs

use crate::state::AppState;
use crate::api_client::ApiClient;
use serde::{Deserialize, Serialize};
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

    loop {
        tokio::select! {
            Some(request) = order_rx.recv() => {
                info!("[OrderWatcher] Received request to watch order ID: {} (clientOid: {})", request.order_id, request.client_oid);
                let client_clone = api_client.clone();
                let tx_clone = order_filled_tx.clone();
                // Запускаем для каждого ордера свою задачу, которая будет его опрашивать
                tokio::spawn(async move {
                    track_order(request, client_clone, tx_clone).await;
                });
            },
            _ = shutdown.notified() => {
                info!("[OrderWatcher] Shutdown signal received. Exiting.");
                break;
            }
        }
    }
    warn!("[OrderWatcher] Channel closed. Service is shutting down.");
}

/// Отслеживает один конкретный ордер до его исполнения или отмены.
async fn track_order(
    req: WatchOrderRequest,
    api_client: Arc<ApiClient>,
    order_filled_tx: mpsc::Sender<OrderFilledEvent>, // <-- ИЗМЕНЕНИЕ: Принимаем MPSC Sender
) {
    let mut poll_interval = interval(Duration::from_millis(500)); // Опрашиваем каждые 500 мс

    loop {
        poll_interval.tick().await;

        let check_result = match req.order_type {
            OrderType::Spot => api_client.get_spot_order(&req.order_id).await,
            OrderType::Futures => api_client.get_futures_order(&req.symbol, &req.order_id).await,
        };

        if let Ok(order_info) = check_result {
            if order_info.status == "filled" {
                info!("[OrderTracker] Order {} for {} FILLED. (clientOid: {})", &req.order_id, &req.symbol, &req.client_oid);
                
                let filled_event = OrderFilledEvent {
                    symbol: req.symbol.clone(),
                    order_id: req.order_id.clone(),
                    order_type: req.order_type,
                    avg_price: order_info.price_avg,
                    base_volume: order_info.base_volume,
                    quote_volume: order_info.quote_volume,
                    client_oid: req.client_oid.clone(),
                    context: req.context,
                };
                
                // Отправляем событие в PositionManager через MPSC канал
                if let Err(e) = order_filled_tx.send(filled_event).await {
                    error!("[OrderWatcher] Failed to send OrderFilledEvent for order {} to PositionManager: {}", &req.order_id, e);
                }
                
                // Завершаем отслеживание этого ордера
                break; // Завершаем отслеживание этого ордера
            }
        }
        // Если ордер еще не исполнен или произошла ошибка API (например, временная недоступность),
        // мы просто продолжаем цикл и попробуем снова через 500 мс.
        // Логирование здесь избыточно, т.к. это нормальное состояние.
    }
}