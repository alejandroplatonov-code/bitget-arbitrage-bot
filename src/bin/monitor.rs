// src/bin/monitor.rs

use futures_util::StreamExt;
use rust_template_for_testing::types::{ActivePosition, CompletedTrade, TradeEvent};
use std::collections::HashMap;
use crossterm::{cursor, execute, terminal::{Clear, ClearType}};
use std::io::{stdout, Write};
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Инициализация логирования
    let subscriber = FmtSubscriber::builder().with_max_level(Level::INFO).finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    // Локальное хранилище для монитора
    let mut active_positions: HashMap<String, ActivePosition> = HashMap::new();
    let mut completed_trades: Vec<CompletedTrade> = Vec::new();

    let redis_client = redis::Client::open("redis://127.0.0.1/")?;
    // For Pub/Sub, a dedicated connection is required. `get_async_connection` is deprecated
    // but is the correct function to use for a connection that will be converted to Pub/Sub.
    #[allow(deprecated)]
    let mut pubsub = redis_client.get_tokio_connection().await?.into_pubsub();

    info!("Successfully connected to Redis for PubSub.");
    pubsub.subscribe("trade_events").await?;
    info!("Subscribed to 'trade_events'. Listening for messages...");

    let mut msg_stream = pubsub.on_message();

    loop {
        if let Some(msg) = msg_stream.next().await {
            info!("[Monitor] Received a message from Redis channel."); // <-- ЛОГ 1

            if let Ok(payload) = msg.get_payload::<String>() {
                info!("[Monitor] Payload: {}", payload); // <-- ЛОГ 2

                if let Ok(event) = serde_json::from_str::<TradeEvent>(&payload) {
                    info!("[Monitor] Successfully parsed event."); // <-- ЛОГ 3
                    match event {
                        TradeEvent::PositionOpened(pos) => {
                            info!("[Monitor] Event: PositionOpened for {}", pos.symbol);
                            active_positions.insert(pos.symbol.clone(), pos);
                        },
                        TradeEvent::PositionClosed(trade) => {
                            info!("[Monitor] Event: PositionClosed for {}", trade.entry_data.symbol);
                            active_positions.remove(&trade.entry_data.symbol);
                            completed_trades.push(trade);
                            // Сортируем (новые вверху) и оставляем только последние 10
                            completed_trades.sort_by(|a, b| b.exit_time.cmp(&a.exit_time));
                            completed_trades.truncate(10);
                        }
                    }
                } else {
                    error!("[Monitor] FAILED to parse TradeEvent from payload."); // <-- ЛОГ 4
                }
            } else {
                error!("[Monitor] FAILED to get payload from message."); // <-- ЛОГ 5
            }
            // После каждого события перерисовываем экран
            redraw_terminal(&active_positions, &completed_trades);
        }
    }
}

// Функция для красивого вывода в консоль
fn redraw_terminal(active: &HashMap<String, ActivePosition>, completed: &[CompletedTrade]) {
    let mut out = stdout();
    // Очищаем терминал и перемещаем курсор в начало
    execute!(out, Clear(ClearType::All), cursor::MoveTo(0, 0)).unwrap();
    
    writeln!(out, "--- Trade Monitor ---\n").unwrap();
    writeln!(out, "--- Active Positions ({}) ---", active.len()).unwrap();
    if active.is_empty() {
        writeln!(out, "None").unwrap();
    } else {
        // --- ИЗМЕНЕННЫЙ ЗАГОЛОВОК ---
        writeln!(out, "{:<15} | ENTRY SPREAD (%)", "PAIR").unwrap();
        writeln!(out, "{:-<15}-|------------------", "").unwrap();
        for pos in active.values() {
            // --- ИЗМЕНЕННЫЙ ВЫВОД ---
            writeln!(out, "{:<15} | {:>16.4}%", pos.symbol, pos.entry_spread_percent).unwrap();
        }
    }
    
    writeln!(out, "\n--- Last 10 Completed Trades ({}) ---", completed.len()).unwrap();
    if completed.is_empty() {
        writeln!(out, "None").unwrap();
    } else {
        // --- ИЗМЕНЕННЫЙ ЗАГОЛОВОК ---
        writeln!(out, "{:<15} | ENTRY SPREAD | EXIT SPREAD  | PNL (USDT)", "PAIR").unwrap();
        writeln!(out, "{:-<15}-|--------------|--------------|-------------", "").unwrap();
        for trade in completed {
            // --- ИЗМЕНЕННЫЙ ВЫВОД ---
            writeln!(
                out,
                "{:<15} | {:>12.4}% | {:>12.4}% | {:>11.4}",
                trade.entry_data.symbol, trade.entry_spread_percent, trade.exit_spread_percent, trade.final_pnl
            ).unwrap();
        }
    }
    out.flush().unwrap();
}