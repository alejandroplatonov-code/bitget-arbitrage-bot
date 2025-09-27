// src/order_watcher.rs

use crate::state::AppState;
use crate::api_client::ApiClient;
use serde::{Deserialize, Serialize};
use crate::utils::send_cancellable;
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

    // --- ИЗМЕНЕНИЕ 1: Создаем JoinSet для управления задачами-трекерами ---
    let mut tracking_tasks = JoinSet::new();

    loop {
        tokio::select! {
            // Ветка 1: Получаем новый ордер для отслеживания
            Some(request) = order_rx.recv() => {
                info!("[OrderWatcher] Received request to watch order ID: {} (clientOid: {})", request.order_id, request.client_oid);
                let client_clone = api_client.clone();
                let tx_clone = order_filled_tx.clone();
                let shutdown_clone = shutdown.clone(); // Клонируем для задачи

                // --- ИЗМЕНЕНИЕ 2: Запускаем трекер внутри управляемого JoinSet ---
                tracking_tasks.spawn(async move {
                    track_order(request, client_clone, tx_clone, shutdown_clone).await;
                });
            },

            // Ветка 2: Периодически очищаем JoinSet от уже завершившихся задач (когда ордер исполнился)
            // Это необязательно для исправления бага, но является хорошей практикой для предотвращения утечек памяти.
            Some(_) = tracking_tasks.join_next(), if !tracking_tasks.is_empty() => {},

            // Ветка 3: Получаем сигнал о завершении работы
            _ = shutdown.notified() => {
                info!("[OrderWatcher] Shutdown signal received. Aborting all active order trackers.");
                // --- ИЗМЕНЕНИЕ 3 (КЛЮЧЕВОЕ): Принудительно завершаем все дочерние задачи ---
                tracking_tasks.abort_all();
                break; // Выходим из главного цикла
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
) {
    let mut poll_interval = interval(Duration::from_millis(500)); // Опрашиваем каждые 500 мс

    loop {
        // --- ИЗМЕНЕНИЕ: Используем select! для ожидания тика или shutdown ---
        tokio::select! {
            _ = poll_interval.tick() => {
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
                        // --- ИЗМЕНЕНИЕ: Используем `send_cancellable` ---
                        if !send_cancellable(&order_filled_tx, filled_event, &shutdown).await {
                            error!("[OrderWatcher] Failed to send OrderFilledEvent for order {} to PositionManager (channel closed or shutdown).", &req.order_id);
                        }
                        
                        // Завершаем отслеживание этого ордера
                        break;
                    }
                }
                // Если ордер еще не исполнен или произошла ошибка API (например, временная недоступность),
                // мы просто продолжаем цикл и попробуем снова через 500 мс.
                // Логирование здесь избыточно, т.к. это нормальное состояние.
            },
            _ = shutdown.notified() => {
                info!("[OrderTracker] Shutdown signal received for order {}. Stopping tracking.", req.order_id);
                break;
            }
        }
    }
}