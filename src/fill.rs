//! Paper fills against a real order book snapshot, the way a FAK market order fills: what the
//! book holds inside the price limit is taken, best price first, the rest is cancelled.

/// A taker fill: shares, dollars paid (or received) before fees, and the fee.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Fill {
    pub shares: f64,
    pub usd: f64,
    pub fee: f64,
}

impl Fill {
    pub fn avg(&self) -> Option<f64> {
        (self.shares > 0.0).then(|| self.usd / self.shares)
    }
}

/// Price floor/ceiling on the tick grid: a buy limit rounds down, a sale's up.
pub fn on_tick(p: f64, tick: f64, buy: bool) -> f64 {
    let n = p / tick;
    let n = if buy { (n + 1e-9).floor() } else { (n - 1e-9).ceil() };
    let v = (n * tick * 1e6).round() / 1e6;
    v.clamp(tick, 1.0 - tick)
}

/// The highest price a copied buy may pay: `cap_pct` above the wallet's price on the tick
/// grid, and never less than `min_ticks` ticks above it.
pub fn buy_cap(wallet_price: f64, tick: f64, cap_pct: f64, min_ticks: f64) -> f64 {
    let base = on_tick((wallet_price / tick).round() * tick, tick, true);
    let pct = on_tick(base * (1.0 + cap_pct), tick, true);
    pct.max(on_tick(base + min_ticks * tick, tick, true))
}

/// The lowest price a copied sale may take.
pub fn sell_floor(wallet_price: f64, tick: f64, floor_pct: f64) -> f64 {
    on_tick(wallet_price * (1.0 - floor_pct), tick, false)
}

/// `usd` spent on the asks at or under `cap` (asks best first). `fee(p)` is the per-share fee.
pub fn buy(asks: &[(f64, f64)], usd: f64, cap: f64, fee: impl Fn(f64) -> f64) -> Fill {
    let mut f = Fill::default();
    let mut left = usd;
    for &(p, size) in asks {
        if p > cap + 1e-12 || left <= 1e-9 {
            break;
        }
        let take = (left / p).min(size);
        f.shares += take;
        f.usd += take * p;
        f.fee += take * fee(p);
        left -= take * p;
    }
    f
}

/// `shares` sold into the bids at or above `floor` (bids best first).
pub fn sell(bids: &[(f64, f64)], shares: f64, floor: f64, fee: impl Fn(f64) -> f64) -> Fill {
    let mut f = Fill::default();
    let mut left = shares;
    for &(p, size) in bids {
        if p < floor - 1e-12 || left <= 1e-9 {
            break;
        }
        let take = left.min(size);
        f.shares += take;
        f.usd += take * p;
        f.fee += take * fee(p);
        left -= take;
    }
    f
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cap_is_pct_or_one_tick() {
        assert_eq!(buy_cap(0.35, 0.01, 0.02, 1.0), 0.36); // 2% rounds to 0.35: one tick up
        assert_eq!(buy_cap(0.75, 0.01, 0.02, 1.0), 0.76);
        assert_eq!(buy_cap(0.60, 0.01, 0.10, 1.0), 0.66);
        assert_eq!(buy_cap(0.6099999, 0.01, 0.02, 1.0), 0.62); // the stream's 0.6099.. is the tick 0.61
        assert_eq!(buy_cap(0.99, 0.01, 0.02, 1.0), 0.99);
        assert_eq!(sell_floor(0.40, 0.01, 0.05), 0.38);
    }

    #[test]
    fn buy_walks_the_book_up_to_the_cap() {
        let asks = [(0.50, 1.0), (0.51, 1.0), (0.60, 100.0)];
        let f = buy(&asks, 1.0, 0.51, |_| 0.0);
        assert!((f.shares - 1.0 - 0.5 / 0.51).abs() < 1e-9);
        assert!((f.usd - 1.0).abs() < 1e-9);
        // nothing under the cap: no fill (a FAK is killed)
        assert_eq!(buy(&[(0.70, 10.0)], 1.0, 0.51, |_| 0.0).shares, 0.0);
        // a thin book: partial fill
        let p = buy(&[(0.50, 0.5)], 1.0, 0.51, |_| 0.0);
        assert!((p.usd - 0.25).abs() < 1e-9);
    }

    #[test]
    fn sell_stops_at_the_floor_and_charges_fees() {
        let bids = [(0.40, 1.0), (0.39, 1.0), (0.30, 5.0)];
        let f = sell(&bids, 3.0, 0.38, |p| 0.07 * p * (1.0 - p));
        assert!((f.shares - 2.0).abs() < 1e-9);
        assert!((f.usd - 0.79).abs() < 1e-9);
        assert!(f.fee > 0.0);
    }
}
