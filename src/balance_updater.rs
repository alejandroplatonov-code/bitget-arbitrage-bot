// /home/platon/Bot_Bit_Get_Rust/src/balance_updater.rs
use crate::{api_client::ApiClient, state::AppState, types::BalanceCacheState};
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;
use tracing::{info, warn};

const UPDATE_INTERVAL: Duration = Duration::from_secs(5); // Обновляем балансы каждые 5 секунд

pub async fn run_balance_updater(
    app_state: Arc<AppState>,
    api_client: Arc<ApiClient>,
    shutdown: Arc<Notify>,
) {
    info!("[BalanceUpdater] Service started.");
    let mut interval = tokio::time::interval(UPDATE_INTERVAL);

    loop {
        tokio::select! {
            _ = interval.tick() => {
                // Копируем список активных позиций, чтобы не держать лок долго
                let positions_to_update: Vec<_> = app_state.inner.active_positions.iter()
                    .map(|entry| entry.value().clone())
                    .collect();

                if positions_to_update.is_empty() {
                    continue;
                }

                info!("[BalanceUpdater] Updating balances for {} active position(s)...", positions_to_update.len());

                for position in positions_to_update {
                    let client = api_client.clone();
                    // Запускаем обновление для каждой позиции в отдельной под-задаче
                    tokio::spawn(async move {
                        let base_coin = position.symbol.replace("USDT", "");
                        match client.get_spot_balance(&base_coin).await {
                            Ok(balance) => {
                                // Записываем полученный баланс в новый кэш
                                *position.balance_cache.lock().unwrap() = BalanceCacheState::Cached(balance);
                            },
                            Err(e) => {
                                warn!("[BalanceUpdater] Failed to update balance for {}: {:?}", position.symbol, e);
                            }
                        }
                    });
                }
            },
            _ = shutdown.notified() => {
                info!("[BalanceUpdater] Shutdown signal received. Exiting.");
                break;
            }
        }
    }
    info!("[BalanceUpdater] Service has shut down.");
}