// src/utils.rs

use tokio::sync::mpsc;
use tokio::sync::Notify;
use std::sync::Arc;
use tracing::warn;

/// Отправляет сообщение в MPSC-канал, но прерывает операцию, если получен сигнал shutdown.
pub async fn send_cancellable<T>(
    tx: &mpsc::Sender<T>,
    message: T,
    shutdown: &Arc<Notify>,
) -> bool {
    tokio::select! {
        res = tx.send(message) => {
            if res.is_err() {
                warn!("Failed to send message: channel closed.");
                false
            } else {
                true
            }
        },
        _ = shutdown.notified() => {
            warn!("Shutdown signal received while trying to send a message.");
            false
        }
    }
}