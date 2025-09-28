// src/bin/top_spreads.rs

use rust_template_for_testing::config::load_token_list;
use rust_template_for_testing::connectors::bitget::BitgetConnector;
use rust_template_for_testing::state::AppState;
use rust_template_for_testing::trading_logic;
use crossterm::{
    cursor, execute,
    terminal::{Clear, ClearType},
};
use rust_decimal::Decimal;
use std::io::{stdout, Write};
use std::sync::Arc; 
use std::time::{Duration};
use tokio::sync::mpsc;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    info!("--- Top 5 Spreads Monitor ---");

    let app_state = AppState::new();
    let pairs = load_token_list()?;
    info!("Loaded {} pairs. Starting connectors...", pairs.len());

    // Создаем "пустышку" для канала, так как коннектору он нужен,
    // но в этом приложении мы не используем сигналы об обновлении.
    let (dummy_tx, _) = mpsc::channel::<String>(1);

    // --- ЗАПУСК КОННЕКТОРОВ ---
    let spot_connector = BitgetConnector::new(
        "SPOT".to_string(),
        pairs.clone(),
        app_state.inner.clone(),
        vec!["books".to_string()], // Subscribe only to books
        dummy_tx.clone(),
    );
    tokio::spawn(async move { spot_connector.run(Arc::new(tokio::sync::Notify::new())).await });

    let futures_connector = BitgetConnector::new(
        "USDT-FUTURES".to_string(),
        pairs,
        app_state.inner.clone(),
        vec!["books".to_string()], // Subscribe only to books
        dummy_tx,
    );
    tokio::spawn(async move { futures_connector.run(Arc::new(tokio::sync::Notify::new())).await });

    info!("Connectors spawned. Waiting for data...");

    let mut interval = tokio::time::interval(Duration::from_secs(1)); // Обновляем экран раз в секунду
    let hundred = Decimal::from(100);
    let trade_amount = Decimal::from(100); // Расчет спреда для объема в 100 USDT

    loop {
        interval.tick().await;

        let mut all_spreads: Vec<(String, Decimal)> = app_state
            .inner
            .market_data
            .iter()
            .filter_map(|entry| {
                let symbol = entry.key();
                let pair_data = entry.value();

                // 1. Берем лучшую цену из стакана фьючерсов для расчета N.
                // Это надежнее, чем ждать last_price из канала trades.
                if let Some((&last_price, _)) = pair_data.futures_book.asks.iter().next() {
                    if !last_price.is_zero() {
                        let base_qty_n = trade_amount / last_price;

                        // 2. Рассчитываем VWAP для этого N
                        if let Some(sell_futures_res) = trading_logic::calculate_revenue_from_sale(&pair_data.futures_book.bids, base_qty_n) {
                            if let Some(buy_spot_res) = trading_logic::calculate_cost_to_acquire(&pair_data.spot_book.asks, base_qty_n) {
                                if !buy_spot_res.total_quote_qty.is_zero() {
                                    // 3. Проверяем спред
                                    let spread_percent = ((sell_futures_res.total_quote_qty - buy_spot_res.total_quote_qty) / buy_spot_res.total_quote_qty) * hundred;
                                    return Some((symbol.clone(), spread_percent));
                                }
                            }
                        }
                    }
                }
                None
            })
            .collect();

        all_spreads.sort_by(|a, b| b.1.cmp(&a.1));
        redraw_terminal(&all_spreads);
    }
}

fn redraw_terminal(spreads: &[(String, Decimal)]) {
    let mut out = stdout();
    execute!(out, Clear(ClearType::All), cursor::MoveTo(0, 0)).unwrap();

    writeln!(out, "--- Top 5 Positive Spreads (updated every second) ---\n").unwrap();

    let top_5: Vec<_> = spreads.iter().filter(|(_, s)| s.is_sign_positive()).take(5).collect();

    if top_5.is_empty() {
        writeln!(out, "No positive spreads found at the moment.").unwrap();
    } else {
        writeln!(out, "{:<15} | {:<10}", "PAIR", "SPREAD (%)").unwrap();
        writeln!(out, "{:-<15}-|-{:-<10}", "", "").unwrap();
        for (symbol, spread) in top_5 {
            writeln!(out, "{:<15} | {:>9.4}%", symbol, spread).unwrap();
        }
    }
    out.flush().unwrap();
}