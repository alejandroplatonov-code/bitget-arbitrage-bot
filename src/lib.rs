// src/lib.rs

pub mod algorithm;
pub mod api_client; // Already public
pub mod config;
pub mod connectors;
pub mod error;
pub mod orderbook;
pub mod order_watcher;
pub mod position_manager;
pub mod state;
pub mod trading_logic;
pub mod types;

use std::fs::File;
use std::io::{self, BufRead, BufReader};

/// Loads a list of tokens from a text file, one token per line.
pub fn load_token_list(path: &str) -> io::Result<Vec<String>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    reader.lines().collect()
}