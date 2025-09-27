// src/trading_logic.rs

use rust_decimal::Decimal;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VwapResult {
    pub total_quote_qty: Decimal, // Общая сумма в USDT (затраты или выручка)
    pub total_base_qty: Decimal,  // Общее количество базового актива (например, BTC)
    pub vwap: Decimal,            // Рассчитанная средневзвешенная цена
}

/// Назначение: Реализация пункта 2.2 математической модели. Рассчитывает, сколько USDT нужно потратить (C_entry или C_exit),
/// чтобы купить заданное количество N базового актива.
///
/// # Arguments
///
/// * `asks` - Стакан асков (цены предложения), где ключ - цена, значение - объем.
/// * `base_quantity_to_buy` - Количество базового актива (например, BTC), которое нужно купить.
///
/// # Returns
///
/// * `Some(VwapResult)` - если в стакане достаточно ликвидности.
/// * `None` - если ликвидности не хватает.
pub fn calculate_cost_to_acquire(
    asks: &BTreeMap<Decimal, Decimal>,
    base_quantity_to_buy: Decimal,
) -> Option<VwapResult> {
    if base_quantity_to_buy.is_zero() || base_quantity_to_buy.is_sign_negative() {
        return None;
    }

    let mut total_quote_qty = Decimal::ZERO;
    let mut total_base_qty = Decimal::ZERO;
    let mut remaining_base_to_buy = base_quantity_to_buy;

    // Итерируемся по аскам от самой низкой цены (лучшей для покупки) к высокой.
    for (&price, &size) in asks.iter() {
        if remaining_base_to_buy <= size {
            // Этот уровень цен полностью покрывает оставшуюся потребность.
            total_quote_qty += remaining_base_to_buy * price;
            total_base_qty += remaining_base_to_buy;
            remaining_base_to_buy = Decimal::ZERO;
            break; // Мы купили все, что хотели.
        } else {
            // Забираем весь объем с этого уровня цен и переходим к следующему.
            total_quote_qty += size * price;
            total_base_qty += size;
            remaining_base_to_buy -= size;
        }
    }

    if remaining_base_to_buy.is_zero() {
        let vwap = total_quote_qty / total_base_qty;
        Some(VwapResult {
            total_quote_qty,
            total_base_qty,
            vwap,
        })
    } else {
        // Ликвидности в стакане не хватило.
        None
    }
}

/// Назначение: Реализация пункта 3.1 модели (расчет R_exit). Рассчитывает, сколько USDT можно получить
/// от продажи заданного количества N базового актива.
///
/// # Arguments
///
/// * `bids` - Стакан бидов (цены спроса), где ключ - цена, значение - объем.
/// * `base_quantity_to_sell` - Количество базового актива (например, BTC), которое нужно продать.
///
/// # Returns
///
/// * `Some(VwapResult)` - если в стакане достаточно ликвидности для продажи.
/// * `None` - если ликвидности не хватает.
pub fn calculate_revenue_from_sale(
    bids: &BTreeMap<Decimal, Decimal>,
    base_quantity_to_sell: Decimal,
) -> Option<VwapResult> {
    if base_quantity_to_sell.is_zero() || base_quantity_to_sell.is_sign_negative() {
        return None;
    }

    let mut total_quote_qty = Decimal::ZERO;
    let mut total_base_qty = Decimal::ZERO;
    let mut remaining_base_to_sell = base_quantity_to_sell;

    // Итерируемся по бидам от самой высокой цены (лучшей для продажи) к низкой.
    for (&price, &size) in bids.iter().rev() {
        if remaining_base_to_sell <= size {
            // Этот уровень цен полностью покрывает оставшуюся потребность в продаже.
            total_quote_qty += remaining_base_to_sell * price;
            total_base_qty += remaining_base_to_sell;
            remaining_base_to_sell = Decimal::ZERO;
            break; // Мы продали все, что хотели.
        } else {
            // Продаем весь объем на этом уровне цен и переходим к следующему.
            total_quote_qty += size * price;
            total_base_qty += size;
            remaining_base_to_sell -= size;
        }
    }

    if remaining_base_to_sell.is_zero() {
        let vwap = total_quote_qty / total_base_qty;
        Some(VwapResult {
            total_quote_qty,
            total_base_qty,
            vwap,
        })
    } else {
        // Ликвидности в стакане не хватило.
        None
    }
}

/// Назначение: Реализация пункта 2.1 модели. Рассчитывает, сколько базового актива N нужно продать,
/// чтобы получить целевую выручку V_target.
///
/// # Arguments
///
/// * `bids` - Стакан бидов (цены спроса).
/// * `quote_quantity_to_receive` - Целевая сумма в USDT, которую мы хотим получить.
///
/// # Returns
///
/// * `Some(VwapResult)` - если в стакане достаточно ликвидности для получения целевой выручки.
/// * `None` - если ликвидности не хватает.
pub fn calculate_qty_for_target_revenue(
    bids: &BTreeMap<Decimal, Decimal>,
    quote_quantity_to_receive: Decimal,
) -> Option<VwapResult> {
    if quote_quantity_to_receive.is_zero() || quote_quantity_to_receive.is_sign_negative() {
        return None;
    }

    let mut total_quote_qty = Decimal::ZERO;
    let mut total_base_qty = Decimal::ZERO;
    let mut remaining_quote_to_receive = quote_quantity_to_receive;

    // Итерируемся по бидам от самой высокой цены (лучшей для продажи) к низкой.
    for (&price, &size) in bids.iter().rev() {
        let quote_at_level = size * price;

        if remaining_quote_to_receive <= quote_at_level {
            // Выручки на этом уровне достаточно для достижения цели.
            // Рассчитываем, какую часть объема (base) нужно продать.
            let partial_base_qty = remaining_quote_to_receive / price;
            total_base_qty += partial_base_qty;
            total_quote_qty += remaining_quote_to_receive; // или partial_base_qty * price
            remaining_quote_to_receive = Decimal::ZERO;
            break; // Цель достигнута.
        } else {
            // Забираем всю ликвидность с этого уровня и идем дальше.
            total_quote_qty += quote_at_level;
            total_base_qty += size;
            remaining_quote_to_receive -= quote_at_level;
        }
    }

    if remaining_quote_to_receive.is_zero() {
        let vwap = total_quote_qty / total_base_qty;
        Some(VwapResult {
            total_quote_qty,
            total_base_qty,
            vwap,
        })
    } else {
        // Ликвидности в стакане не хватило.
        None
    }
}

/// Округляет (отбрасывает) Decimal до указанного количества знаков после запятой.
///
/// # Arguments
/// * `qty` - Исходное значение.
/// * `scale` - Количество знаков после запятой, которое нужно оставить.
pub fn round_down(qty: Decimal, scale: u32) -> Decimal {
    // `trunc_with_scale` делает именно то, что нам нужно - отбрасывает лишние знаки.
    qty.trunc_with_scale(scale)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::dec;

    /// Helper to create a BTreeMap from a slice of tuples.
    fn create_book_from_slice(data: &[(Decimal, Decimal)]) -> BTreeMap<Decimal, Decimal> {
        data.iter().cloned().collect()
    }

    // --- Tests for calculate_cost_to_acquire ---

    #[test]
    fn test_cost_to_acquire_sufficient_liquidity() {
        let asks = create_book_from_slice(&[
            (dec!(101), dec!(10)), // Level 2
            (dec!(100), dec!(5)),  // Level 1 (best price)
            (dec!(102), dec!(20)), // Level 3
        ]);
        let quantity_to_buy = dec!(7); // Buy all of level 1 and part of level 2

        let result = calculate_cost_to_acquire(&asks, quantity_to_buy).unwrap();

        // Expected cost: (5 * 100) + (2 * 101) = 500 + 202 = 702
        let expected_cost = dec!(702);
        // Expected VWAP: 702 / 7 = 100.285714...
        let expected_vwap = expected_cost / quantity_to_buy;

        assert_eq!(result.total_base_qty, quantity_to_buy);
        assert_eq!(result.total_quote_qty, expected_cost);
        assert_eq!(result.vwap, expected_vwap);
    }

    #[test]
    fn test_cost_to_acquire_insufficient_liquidity() {
        let asks = create_book_from_slice(&[
            (dec!(100), dec!(5)),
            (dec!(101), dec!(10)),
        ]);
        let quantity_to_buy = dec!(20); // More than available liquidity (15)

        let result = calculate_cost_to_acquire(&asks, quantity_to_buy);

        assert!(result.is_none());
    }

    #[test]
    fn test_cost_to_acquire_zero_quantity() {
        let asks = create_book_from_slice(&[(dec!(100), dec!(5))]);
        assert!(calculate_cost_to_acquire(&asks, dec!(0)).is_none());
    }

    // --- Tests for calculate_revenue_from_sale ---

    #[test]
    fn test_revenue_from_sale_sufficient_liquidity() {
        let bids = create_book_from_slice(&[
            (dec!(98), dec!(20)), // Level 3
            (dec!(99), dec!(10)),  // Level 2
            (dec!(100), dec!(5)), // Level 1 (best price)
        ]);
        let quantity_to_sell = dec!(8); // Sell all of level 1 and part of level 2

        let result = calculate_revenue_from_sale(&bids, quantity_to_sell).unwrap();

        // Expected revenue: (5 * 100) + (3 * 99) = 500 + 297 = 797
        let expected_revenue = dec!(797);
        // Expected VWAP: 797 / 8 = 99.625
        let expected_vwap = expected_revenue / quantity_to_sell;

        assert_eq!(result.total_base_qty, quantity_to_sell);
        assert_eq!(result.total_quote_qty, expected_revenue);
        assert_eq!(result.vwap, expected_vwap);
    }

    #[test]
    fn test_revenue_from_sale_insufficient_liquidity() {
        let bids = create_book_from_slice(&[
            (dec!(100), dec!(5)),
            (dec!(99), dec!(10)),
        ]);
        let quantity_to_sell = dec!(20); // More than available liquidity (15)

        let result = calculate_revenue_from_sale(&bids, quantity_to_sell);

        assert!(result.is_none());
    }

    #[test]
    fn test_revenue_from_sale_exact_liquidity() {
        let bids = create_book_from_slice(&[
            (dec!(100), dec!(5)),
            (dec!(99), dec!(10)),
        ]);
        let quantity_to_sell = dec!(15);

        let result = calculate_revenue_from_sale(&bids, quantity_to_sell).unwrap();
        let expected_revenue = dec!(5) * dec!(100) + dec!(10) * dec!(99); // 500 + 990 = 1490

        assert_eq!(result.total_quote_qty, expected_revenue);
        assert_eq!(result.total_base_qty, quantity_to_sell);
    }

    // --- Tests for calculate_qty_for_target_revenue ---

    #[test]
    fn test_qty_for_target_revenue_sufficient_liquidity() {
        let bids = create_book_from_slice(&[
            (dec!(98), dec!(20)), // Level 3
            (dec!(99), dec!(10)),  // Level 2
            (dec!(100), dec!(5)), // Level 1 (best price)
        ]);
        // Target revenue: 700 USDT.
        // Level 1 (price 100, size 5) gives 500 USDT. Need 200 more.
        // From Level 2 (price 99), we need to sell 200 / 99 = 2.0202... base qty.
        let target_revenue = dec!(700);

        let result = calculate_qty_for_target_revenue(&bids, target_revenue).unwrap();

        // Expected base qty: 5 (from level 1) + (200 / 99)
        let expected_base_qty = dec!(5) + (dec!(200) / dec!(99));
        // Expected VWAP: 700 / expected_base_qty
        let expected_vwap = target_revenue / expected_base_qty;

        // We need to be careful with precision here.
        // Let's check the total quote quantity is what we asked for.
        assert_eq!(result.total_quote_qty, target_revenue);
        assert_eq!(result.total_base_qty, expected_base_qty);
        assert_eq!(result.vwap, expected_vwap);
    }

    #[test]
    fn test_qty_for_target_revenue_insufficient_liquidity() {
        let bids = create_book_from_slice(&[
            (dec!(100), dec!(5)), // Max revenue: 500
            (dec!(99), dec!(10)),  // Max revenue: 990. Total: 1490
        ]);
        let target_revenue = dec!(2000); // More than available

        let result = calculate_qty_for_target_revenue(&bids, target_revenue);

        assert!(result.is_none());
    }

    #[test]
    fn test_qty_for_target_revenue_exact_liquidity() {
        let bids = create_book_from_slice(&[
            (dec!(100), dec!(5)),
            (dec!(99), dec!(10)),
        ]);
        // Total available revenue is (5 * 100) + (10 * 99) = 500 + 990 = 1490
        let target_revenue = dec!(1490);

        let result = calculate_qty_for_target_revenue(&bids, target_revenue).unwrap();

        // Expected base qty is total size: 5 + 10 = 15
        let expected_base_qty = dec!(15);
        let expected_vwap = target_revenue / expected_base_qty;

        assert_eq!(result.total_quote_qty, target_revenue);
        assert_eq!(result.total_base_qty, expected_base_qty);
        assert_eq!(result.vwap, expected_vwap);
    }

    // --- Tests for round_down ---
    #[test]
    fn test_round_down() {
        assert_eq!(round_down(dec!(123.45678), 0), dec!(123));
        assert_eq!(round_down(dec!(123.45678), 2), dec!(123.45));
        assert_eq!(round_down(dec!(123.45678), 4), dec!(123.4567));
        assert_eq!(round_down(dec!(123.45678), 8), dec!(123.45678));
        assert_eq!(round_down(dec!(99), 3), dec!(99));
        let qty = dec!(0.123456789);
        let scale = 3;
        assert_eq!(round_down(qty, scale), dec!(0.123));
    }
}