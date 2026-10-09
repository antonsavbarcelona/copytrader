//! Run settings. The defaults are the rules the live $1 copy test of 2026-10-01 ended on.

use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct Config {
    /// Where events.jsonl, state.json and wallets.json are kept.
    pub data_dir: PathBuf,
    /// Dollars per copied buy (a market order: the exchange's minimum is $1).
    pub stake_usd: f64,
    /// A buy takes the book up to this much above the wallet's price ...
    pub buy_cap_pct: f64,
    /// ... and always at least one tick above it (on cheap outcomes the % rounds to nothing) ...
    pub buy_cap_min_ticks: f64,
    /// ... and at least this many dollars a share above it (0: off). The live copy's slippage.
    pub buy_cap_abs: f64,
    /// A sale takes the bids down to this much under the wallet's price.
    pub sell_floor_pct: f64,
    /// Fills of one wallet order arriving within this window are one trade.
    pub join_s: f64,
    /// One buy copied per wallet and market in this window (a match is traded for hours,
    /// and wallets trading both sides in a loop would take every copy).
    pub market_gap_s: f64,
    /// A buy first seen over REST later than this after the wallet's trade is too late to copy.
    pub rest_max_age_s: f64,
    /// Once the positions API has caught up after a wallet's sale, ours is set to the share
    /// it kept.
    pub recheck_s: f64,
    /// Pause between two wallets' REST read-backs (the whole list is read in turn).
    pub poll_gap_ms: u64,
    /// How often the Polyfox leaderboard is read again for new wallets.
    pub wallets_refresh_s: u64,
    /// How often held outcomes are checked for settlement.
    pub settle_every_s: u64,
    /// Real orders (live.rs) instead of paper fills, events to their own tables.
    pub live: bool,
    /// Live: copy only the `top_n` wallets whose paper copies did best over `top_days`
    /// (positive return, at least `top_min_closed` closed positions), picked again every
    /// `wallets_refresh_s`.
    pub top_n: i64,
    pub top_days: f64,
    pub top_min_closed: i64,
    /// The live copy pays up to this much a share over the wallet's price.
    pub live_cap_abs: f64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data"),
            stake_usd: 1.0,
            buy_cap_pct: 0.02,
            buy_cap_min_ticks: 1.0,
            buy_cap_abs: 0.0,
            sell_floor_pct: 0.05,
            join_s: 5.0,
            market_gap_s: 6.0 * 3600.0,
            rest_max_age_s: 60.0,
            recheck_s: 90.0,
            poll_gap_ms: 700,
            wallets_refresh_s: 6 * 3600,
            settle_every_s: 300,
            live: false,
            top_n: 10,
            top_days: 3.0,
            top_min_closed: 15,
            live_cap_abs: 0.03,
        }
    }
}

impl Config {
    /// `--data DIR --stake 1 --cap 0.02 --sell-floor 0.05 --live --top 10 --top-days 3` over
    /// the defaults.
    pub fn from_args(args: &[String]) -> anyhow::Result<Self> {
        let mut c = Self::default();
        let mut i = 0;
        while i < args.len() {
            let v = || args.get(i + 1).cloned().ok_or_else(|| anyhow::anyhow!("{} needs a value", args[i]));
            if args[i] == "--live" {
                c.live = true;
                i += 1;
                continue;
            }
            match args[i].as_str() {
                "--data" => c.data_dir = PathBuf::from(v()?),
                "--stake" => c.stake_usd = v()?.parse()?,
                "--cap" => c.buy_cap_pct = v()?.parse()?,
                "--live-cap-abs" => c.live_cap_abs = v()?.parse()?,
                "--sell-floor" => c.sell_floor_pct = v()?.parse()?,
                "--market-gap-h" => c.market_gap_s = v()?.parse::<f64>()? * 3600.0,
                "--top" => c.top_n = v()?.parse()?,
                "--top-days" => c.top_days = v()?.parse()?,
                "--top-min-closed" => c.top_min_closed = v()?.parse()?,
                other => anyhow::bail!("unknown option {other}"),
            }
            i += 2;
        }
        Ok(c)
    }
}
