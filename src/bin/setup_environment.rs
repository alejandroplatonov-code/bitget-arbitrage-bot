// src/bin/setup_environment.rs

use rust_template_for_testing::api_client::{ApiClient, SetLeverageRequest, SetMarginModeRequest, SetPositionModeRequest};
use rust_template_for_testing::config::Config;
use serde::Deserialize;
use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::time::Duration;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

// --- Структуры для парсинга списков символов ---
#[derive(Deserialize, Debug)]
struct SpotSymbol {
    symbol: String,
    #[serde(rename = "quoteCoin")]
    quote_coin: String,
    status: String,
}
#[derive(Deserialize, Debug)]
struct FuturesSymbol {
    symbol: String,
    #[serde(rename = "symbolStatus")]
    status: String,
}
#[derive(Deserialize, Debug)]
struct ApiResponse<T> {
    code: String,
    msg: String,
    data: T,
}

// --- Часть 1: Сбор и Фильтрация ---
async fn get_common_symbols() -> Result<Vec<String>, Box<dyn std::error::Error>> {
    info!("Fetching SPOT symbols...");
    let spot_response: ApiResponse<Vec<SpotSymbol>> =
        reqwest::get("https://api.bitget.com/api/v2/spot/public/symbols")
            .await?
            .json()
            .await?;
    if spot_response.code != "00000" {
        return Err(format!("Spot API Error: {}", spot_response.msg).into());
    }
    let spot_symbols: HashSet<String> = spot_response
        .data
        .into_iter()
        .filter(|s| s.status == "online" && s.quote_coin == "USDT")
        .map(|s| s.symbol)
        .collect();
    info!("Found {} active USDT spot symbols.", spot_symbols.len());

    info!("Fetching FUTURES symbols...");
    let futures_response: ApiResponse<Vec<FuturesSymbol>> =
        reqwest::get("https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES")
            .await?
            .json()
            .await?;
    if futures_response.code != "00000" {
        return Err(format!("Futures API Error: {}", futures_response.msg).into());
    }
    let futures_symbols: HashSet<String> = futures_response
        .data
        .into_iter()
        .filter(|s| s.status == "normal")
        .map(|s| s.symbol)
        .collect();
    info!("Found {} active USDT-M futures symbols.", futures_symbols.len());

    let mut common_symbols: Vec<String> = spot_symbols.intersection(&futures_symbols).cloned().collect();
    common_symbols.sort();
    info!("Found {} common symbols.", common_symbols.len());

    Ok(common_symbols)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --- Инициализация ---
    let subscriber = FmtSubscriber::builder().with_max_level(Level::INFO).finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    info!("--- Full Environment Setup Script ---");

    // --- Этап 1: Получение и сохранение списка пар ---
    let common_pairs = get_common_symbols().await?;

    let mut file = File::create("tokens.txt")?;
    for symbol in &common_pairs {
        writeln!(file, "{}", symbol)?;
    }
    info!("Successfully saved {} pairs to tokens.txt", common_pairs.len());

    // --- Этап 2: Настройка пар на бирже ---
    info!("Loading API keys to configure pairs...");
    let config = Config::load()?;
    let api_client = ApiClient::new(config.api_keys.api_key, config.api_keys.api_secret, config.api_keys.api_passphrase);

    info!("Setting position mode to One-Way...");
    let mode_req = SetPositionModeRequest {
        product_type: "USDT-FUTURES".to_string(),
        pos_mode: "one_way_mode".to_string(),
    };
    if let Err(e) = api_client.set_position_mode(mode_req).await {
        error!("FATAL: Failed to set One-Way position mode: {}. Please ensure you have no open positions or orders.", e);
        return Err(e.into()); // AppError can be converted into Box<dyn Error>
    }
    info!("Position mode successfully set to One-Way.");

    let leverage_to_set = "1";
    info!("Setting margin mode to ISOLATED and leverage to {}x for all pairs...", leverage_to_set);

    for (i, symbol) in common_pairs.iter().enumerate() {
        info!("[{}/{}] Configuring symbol: {}", i + 1, common_pairs.len(), symbol);

        let margin_req = SetMarginModeRequest { symbol: symbol.clone(), product_type: "USDT-FUTURES".to_string(), margin_mode: "isolated".to_string(), margin_coin: "USDT".to_string() };
        if let Err(e) = api_client.set_margin_mode(margin_req).await {
            error!("\t- Failed to set ISOLATED margin: {}", e);
        } else { info!("\t- Set margin mode to ISOLATED"); }
        tokio::time::sleep(Duration::from_millis(250)).await;

        let lev_req_long = SetLeverageRequest { symbol: symbol.clone(), product_type: "USDT-FUTURES".to_string(), margin_coin: "USDT".to_string(), leverage: leverage_to_set.to_string(), hold_side: Some("long".to_string()) };
        if let Err(e) = api_client.set_leverage(lev_req_long).await { error!("\t- Failed to set LONG leverage: {}", e); } else { info!("\t- Set LONG leverage to {}x", leverage_to_set); }
        tokio::time::sleep(Duration::from_millis(250)).await;

        let lev_req_short = SetLeverageRequest { symbol: symbol.clone(), product_type: "USDT-FUTURES".to_string(), margin_coin: "USDT".to_string(), leverage: leverage_to_set.to_string(), hold_side: Some("short".to_string()) };
        if let Err(e) = api_client.set_leverage(lev_req_short).await { error!("\t- Failed to set SHORT leverage: {}", e); } else { info!("\t- Set SHORT leverage to {}x", leverage_to_set); }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    info!("--- Setup complete! ---");
    Ok(())
}