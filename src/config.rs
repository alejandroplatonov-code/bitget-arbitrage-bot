use serde::Deserialize;
use std::fs;
use crate::error::AppError;
use std::io;
use rust_decimal::Decimal;
use tracing::Level;

// NEW STRUCTURE
#[derive(Deserialize, Debug, Clone, Default)]
pub struct ApiKeys {
    pub api_key: String,
    pub api_secret: String,
    pub api_passphrase: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub trade_amount_usdt: Decimal,
    pub spread_threshold_percent: Decimal,
    pub taker_fee_percent: Decimal,
    pub log_level: String,
    pub ui_update_ms: u64,
    // Процент, который будет добавлен к расчетной стоимости спотового ордера
    // для покрытия проскальзывания. 0.5 означает 0.5%.
    pub spot_slippage_buffer_percent: Decimal,
    pub throttle_ms: u64,
    pub redis_url: String,
    pub live_trading_enabled: bool,

    // НОВОЕ ПОЛЕ
    pub max_active_positions: usize,
    pub position_cooldown_seconds: u64,

    // NEW FIELD
    #[serde(skip)] // This field is not from config.toml
    pub api_keys: ApiKeys,
}

impl Config {
    /// Loads trading parameters from config.toml
    pub fn load() -> Result<Self, AppError> {
        // Step 1: Load the main config
        let config_str = fs::read_to_string("config.toml").map_err(|e| AppError::ConfigError(format!("Could not read config.toml: {}", e)))?;
        let mut config: Config = toml::from_str(&config_str).map_err(|e| AppError::ConfigError(format!("Could not parse config.toml: {}", e)))?;

        // Step 2: Load the secrets
        let secrets_str = fs::read_to_string("secrets.toml")
            .map_err(|e| AppError::ConfigError(format!("Could not read secrets.toml: {}. Make sure the file exists in the project root.", e)))?;
        let api_keys: ApiKeys = toml::from_str(&secrets_str)
            .map_err(|e| AppError::ConfigError(format!("Could not parse secrets.toml: {}", e)))?;
        
        // Step 3: Add the keys to the main structure
        config.api_keys = api_keys;

        Ok(config)
    }

    pub fn get_log_level(&self) -> Level {
        self.log_level.parse().unwrap_or(Level::INFO)
    }

    /// Возвращает комиссию тейкера в виде Decimal (например, 0.001 для 0.1%).
    pub fn taker_fee(&self) -> Decimal {
        self.taker_fee_percent / Decimal::from(100)
    }
}

/// Loads the list of trading pairs from tokens.txt
pub fn load_token_list() -> Result<Vec<String>, io::Error> {
    let content = fs::read_to_string("tokens.txt")?;
    let tokens = content
        .lines()
        // Убираем пробелы, кавычки и запятые
        .map(|s| s.trim().trim_matches(|c| c == '"' || c == ','))
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect();
    Ok(tokens)
}