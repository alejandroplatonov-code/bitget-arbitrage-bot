use crate::types::{ActivePosition, CompletedTrade, PairData, SymbolRules, TradeAnalysisLog, TradingStatus};
use std::sync::atomic::AtomicU8;
use std::sync::Arc;
use std::time::Instant;
use dashmap::{DashMap, DashSet};

/// The inner state of the application, holding all shared data.
#[allow(dead_code)]
pub struct AppStateInner {
    /// Market data for all subscribed pairs. Key: symbol (e.g., "BTCUSDT")
    pub market_data: DashMap<String, PairData>,
    /// Currently open arbitrage positions. Key: symbol (e.g., "BTCUSDT")
    pub active_positions: DashMap<String, ActivePosition>, // TODO: Remove allow(dead_code) when new algorithm uses this
    /// History of completed trades. Key: symbol (e.g., "BTCUSDT")
    pub completed_trades: DashMap<String, Vec<CompletedTrade>>, // TODO: Remove allow(dead_code) when new algorithm uses this
    /// Symbols marked for forced closure by the user.
    pub force_close_requests: DashSet<String>,
    /// Symbols for which a trade entry is currently being attempted.
    pub executing_pairs: DashSet<String>,
    /// Используем AtomicU8, где 0=Running, 1=Stopping. Это сверхбыстро.
    pub trading_status: AtomicU8,
    /// Timestamp of the last check for each pair to enable throttling.
    pub last_checked: DashMap<String, Instant>,
    /// Правила торговли (округление и т.д.) для каждой пары.
    pub symbol_rules: DashMap<String, SymbolRules>,
    /// "Черный ящик" для анализа сделок. Ключ - clientOid.
    pub trade_analysis_logs: DashMap<String, TradeAnalysisLog>,
}

/// A clonable, thread-safe handle to the application's shared state.
#[derive(Clone)]
pub struct AppState {
    pub inner: Arc<AppStateInner>,
}

impl AppState {
    /// Creates a new, empty application state.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(AppStateInner {
                market_data: DashMap::new(),
                active_positions: DashMap::new(),
                completed_trades: DashMap::new(),
                force_close_requests: DashSet::new(),
                executing_pairs: DashSet::new(),
                trading_status: AtomicU8::new(TradingStatus::Running as u8),
                last_checked: DashMap::new(),
                symbol_rules: DashMap::new(),
                trade_analysis_logs: DashMap::new(),
            }),
        }
    }
}