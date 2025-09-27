use serde::Serialize;

#[derive(Serialize, Clone, Debug)]
pub struct PairViewModel {
    pub name: String,
    pub spot_price: String,
    pub futures_price: String,
    pub spread_percent: String,
}

#[derive(Serialize, Clone, Debug)]
pub struct TradeViewModel {
    pub id: String,
    pub pair: String,
    pub direction: String,
    pub status: String,
    pub entry_time: String,
    pub exit_time: String,
    pub entry_spread_percent: String,
    pub current_spread_percent: String, // Добавлено для отображения в UI
    pub entry_price: String,
    pub exit_price: String,
    pub exit_spread_percent: String,
    pub pnl: String,
    pub pnl_percent: String,
}

#[derive(Serialize, Clone, Debug)]
pub struct UiViewModel {
    pub uptime: String,
    pub total_open_sum: String,
    pub net_profit: String,
    pub raw_total_pnl: String,
    pub all_pairs: Vec<PairViewModel>,
    pub open_trades: Vec<TradeViewModel>,
    pub completed_trades: Vec<TradeViewModel>,
    pub pairs_count: usize,
}