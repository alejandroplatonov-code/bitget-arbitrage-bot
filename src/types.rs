use crate::orderbook::OrderBook;
use serde::{Deserialize, Serialize};
use crate::order_watcher::OrderType;
use rust_decimal::Decimal;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Правила округления для пары.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct SymbolRules {
    pub spot_quantity_scale: Option<u32>,
}

/// Represents the synchronization state of an order book for a single instrument.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum SyncState {
    #[default]
    WaitingForSnapshot, // Initial state, waiting for the first full snapshot.
    Synced,             // Snapshot received, the book is ready for updates.
}

/// Represents the market data for a single trading pair (e.g., "BTCUSDT").
/// It contains the order books for both spot and futures markets.
#[allow(dead_code)]
#[derive(Clone)]
pub struct PairData {
    pub spot_book: OrderBook,
    pub futures_book: OrderBook,
    pub spot_last_update: Instant, // TODO: Remove allow(dead_code) when staleness check is implemented
    pub futures_last_update: Instant, // TODO: Remove allow(dead_code) when staleness check is implemented
    // НОВЫЕ ПОЛЯ
    pub spot_last_price: Option<Decimal>,
    pub futures_last_price: Option<Decimal>,
}

impl Default for PairData {
    fn default() -> Self {
        Self {
            spot_book: OrderBook::default(),
            futures_book: OrderBook::default(),
            spot_last_update: Instant::now(),
            futures_last_update: Instant::now(),
            spot_last_price: None,
            futures_last_price: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ArbitrageDirection {
    BuySpotSellFutures,
    BuyFuturesSellSpot,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum TradingStatus {
    Running = 0,
    Stopping = 1,
}

#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PositionState {
    pub current_spread_percent: Decimal,
    pub current_pnl: Decimal,
    pub pnl_percent: Decimal,
}

/// Represents an active, open position for a single trading pair.
#[derive(Debug, Clone, Serialize, Deserialize)] // Already correct, no change needed here.
pub struct ActivePosition {
    pub symbol: String,
    pub direction: ArbitrageDirection,
    pub entry_time: i64,
    pub futures_base_qty: Decimal, // Фактический исполненный объем на фьючерсах
    pub spot_base_qty: Decimal,    // Фактический исполненный объем на споте

    // ПЕРЕИМЕНОВАННЫЕ ПОЛЯ, соответствующие Единой Системе
    pub cost_spot_entry: Decimal,
    pub revenue_futures_entry: Decimal,

    // VWAP-цены (для информации)
    pub spot_entry_vwap: Decimal,   // P_VWAP_spot_entry
    pub futures_entry_vwap: Decimal, // P_VWAP_futures_entry

    // --- НОВОЕ ПОЛЕ ---
    // Хранит состояние, которое постоянно обновляется алгоритмом
    // и читается сервером.
    #[serde(skip, default = "default_position_state")]
    pub current_state: Arc<Mutex<PositionState>>,

    // НОВОЕ ПОЛЕ
    pub entry_spread_percent: Decimal,

    // Временная метка создания позиции для "периода охлаждения"
    #[serde(skip, default = "default_instant")]
    pub created_at: Instant,
}

/// Helper function to provide a default value for the skipped `Instant` field.
fn default_instant() -> Instant {
    Instant::now()
}

/// Represents a trade that has been closed.
#[derive(Debug, Clone, Serialize, Deserialize)] // Already correct, no change needed here.
pub struct CompletedTrade {
    pub entry_data: ActivePosition,

    // Exit parameters
    pub exit_time: i64, // Unix timestamp in milliseconds
    pub cost_exit: Decimal,         // C_exit: Total USDT spent to close
    pub revenue_exit: Decimal,      // R_exit: Total USDT received to close
    pub final_pnl: Decimal,         // The final profit or loss in USDT

    // НОВЫЕ ПОЛЯ
    pub entry_spread_percent: Decimal,
    pub exit_spread_percent: Decimal,
}

/// Describes a task for the compensator module to close an orphaned trade leg.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompensationTask {
    pub symbol: String,
    /// The ID of the order that was successfully placed and now needs compensation.
    pub original_order_id: String,
    /// The quantity of the base asset that was successfully bought or sold.
    pub base_qty_to_compensate: Decimal,
    /// The direction of the original, failed arbitrage attempt.
    /// The compensator will execute the opposite action.
    /// e.g., if original was BuySpot, compensation is SellSpot.
    pub original_direction: ArbitrageDirection,
    /// Which leg of the trade was successfully executed and now needs compensation.
    pub leg_to_compensate: OrderType,
}

/// Событие, которое PositionManager публикует для внешних слушателей (например, монитора).
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "event_type")]
pub enum TradeEvent {
    PositionOpened(ActivePosition),
    PositionClosed(CompletedTrade),
}

/// Helper function to provide a default value for the skipped field.
fn default_position_state() -> Arc<Mutex<PositionState>> {
    Arc::new(Mutex::new(PositionState::default()))
}

/// Represents a command received via the Redis channel.
#[derive(serde::Deserialize, Debug)]
pub struct WsCommand {
    pub action: String,
    pub symbol: Option<String>, // Optional symbol for targeted commands
}