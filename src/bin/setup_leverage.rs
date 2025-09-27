// src/bin/setup_leverage.rs

// Используем общую библиотеку нашего проекта
use rust_template_for_testing::api_client::{ApiClient, SetLeverageRequest, SetMarginModeRequest};
use rust_template_for_testing::config::Config;
use rust_template_for_testing::load_token_list;
use std::time::Duration;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Простая инициализация логгера
    tracing_subscriber::fmt::init();

    info!("--- Leverage & Margin Setup Script ---");

    // 1. Загружаем конфиг, чтобы получить API ключи
    info!("Loading configuration...");
    let config = Config::load()?;
    let api_client = ApiClient::new(
        config.api_keys.api_key,
        config.api_keys.api_secret,
        config.api_keys.api_passphrase,
    );
    info!("API Client created.");

    // 2. Загружаем список пар
    info!("Loading token list from tokens.txt...");
    let pairs = load_token_list("tokens.txt")?;
    info!("Loaded {} pairs to configure.", pairs.len());

    let leverage_to_set = "1"; // Устанавливаем плечо 1x

    // 3. Проходим в цикле по всем парам
    for symbol in &pairs {
        info!("Configuring symbol: {}", symbol);

        // 3.1 Устанавливаем изолированную маржу
        let margin_req = SetMarginModeRequest {
            symbol: symbol.clone(),
            product_type: "USDT-FUTURES".to_string(),
            margin_mode: "isolated".to_string(),
            margin_coin: "USDT".to_string(),
        };
        if let Err(e) = api_client.set_margin_mode(margin_req).await {
            error!("\t- Failed to set ISOLATED margin: {}", e);
            // Продолжаем, даже если ошибка, чтобы попробовать установить плечо
        } else {
            info!("\t- Set margin mode to ISOLATED");
        }

        // Небольшая задержка для соблюдения rate-лимитов
        tokio::time::sleep(Duration::from_millis(250)).await;

        // 3.2 Устанавливаем плечо для long
        let lev_req_long = SetLeverageRequest {
            symbol: symbol.clone(),
            product_type: "USDT-FUTURES".to_string(),
            margin_coin: "USDT".to_string(),
            leverage: leverage_to_set.to_string(),
            hold_side: Some("long".to_string()),
        };
        if let Err(e) = api_client.set_leverage(lev_req_long).await {
            error!("\t- Failed to set LONG leverage: {}", e);
        } else {
            info!("\t- Set LONG leverage to {}x", leverage_to_set);
        }

        tokio::time::sleep(Duration::from_millis(250)).await;

        // 3.3 Устанавливаем плечо для short
        let lev_req_short = SetLeverageRequest {
            symbol: symbol.clone(),
            product_type: "USDT-FUTURES".to_string(),
            margin_coin: "USDT".to_string(),
            leverage: leverage_to_set.to_string(),
            hold_side: Some("short".to_string()),
        };
        if let Err(e) = api_client.set_leverage(lev_req_short).await {
            error!("\t- Failed to set SHORT leverage: {}", e);
        } else {
            info!("\t- Set SHORT leverage to {}x", leverage_to_set);
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    info!("--- Setup complete! ---");
    Ok(())
}