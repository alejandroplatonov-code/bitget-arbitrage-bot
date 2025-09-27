use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::str::FromStr;

#[derive(Deserialize, Debug, Clone)]
pub struct WsOrderBookData {
    pub asks: Vec<[String; 2]>,
    pub bids: Vec<[String; 2]>,
    pub ts: String,
    pub checksum: Option<i32>,
}
#[derive(Default, Clone, Debug)]
pub struct OrderBook {
    pub bids: BTreeMap<Decimal, Decimal>,
    pub asks: BTreeMap<Decimal, Decimal>,
}

impl OrderBook {
    pub fn new() -> Self { Self::default() }

    pub fn apply_snapshot(&mut self, data: &WsOrderBookData) {
        self.bids.clear();
        self.asks.clear();
        for [price, size] in &data.bids {
            if let (Ok(p), Ok(s)) = (Decimal::from_str(price), Decimal::from_str(size)) {
                if !s.is_zero() { self.bids.insert(p, s); }
            }
        }
        for [price, size] in &data.asks {
            if let (Ok(p), Ok(s)) = (Decimal::from_str(price), Decimal::from_str(size)) {
                if !s.is_zero() { self.asks.insert(p, s); }
            }
        }
    }

    pub fn apply_update(&mut self, data: &WsOrderBookData) {
        for [price, size] in &data.bids {
            if let (Ok(p), Ok(s)) = (Decimal::from_str(price), Decimal::from_str(size)) {
                if s.is_zero() { self.bids.remove(&p); } else { self.bids.insert(p, s); }
            }
        }
        for [price, size] in &data.asks {
            if let (Ok(p), Ok(s)) = (Decimal::from_str(price), Decimal::from_str(size)) {
                if s.is_zero() { self.asks.remove(&p); } else { self.asks.insert(p, s); }
            }
        }
    }

    pub fn validate(&self, server_checksum_i32: i32) -> Result<(), String> {
        let local_checksum_u32 = self.calculate_checksum();
        let server_checksum_u32 = server_checksum_i32 as u32;

        if server_checksum_u32 != local_checksum_u32 {
            Err(format!(
                "CHECKSUM MISMATCH! Server: {}, Local: {}",
                server_checksum_i32, local_checksum_u32
            ))
        } else {
            Ok(())
        }
    }

    pub fn calculate_checksum(&self) -> u32 {
        use std::cmp::max;
        let mut hasher = crc32fast::Hasher::new();
        let mut checksum_str = String::with_capacity(1024);

        let bids: Vec<_> = self.bids.iter().rev().take(25).collect();
        let asks: Vec<_> = self.asks.iter().take(25).collect();

        for i in 0..max(bids.len(), asks.len()) {
            if let Some((price, size)) = bids.get(i) {
                checksum_str.push_str(&price.to_string());
                checksum_str.push(':');
                checksum_str.push_str(&size.to_string());
                checksum_str.push(':');
            }
            if let Some((price, size)) = asks.get(i) {
                checksum_str.push_str(&price.to_string());
                checksum_str.push(':');
                checksum_str.push_str(&size.to_string());
                checksum_str.push(':');
            }
        }

        if checksum_str.ends_with(':') {
            checksum_str.pop();
        }

        hasher.update(checksum_str.as_bytes());
        hasher.finalize()
    }
}