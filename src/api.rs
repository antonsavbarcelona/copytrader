//! Polymarket's public HTTP APIs (no keys): the CLOB order book, gamma market metadata, the
//! data API's trades and positions, and the Polyfox smart-wallet leaderboard.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde_json::Value;
use tokio::sync::Mutex;

const CLOB: &str = "https://clob.polymarket.com";
const GAMMA: &str = "https://gamma-api.polymarket.com";
const DATA: &str = "https://data-api.polymarket.com";
const POLYFOX: &str = "https://polyfox.co/api/leaderboard";

#[derive(Clone, Debug)]
pub struct Book {
    /// (price, size), best first.
    pub asks: Vec<(f64, f64)>,
    pub bids: Vec<(f64, f64)>,
    pub tick: f64,
}

#[derive(Clone, Debug)]
#[allow(dead_code)] // condition and question: kept for debugging output
pub struct Market {
    pub condition: String,
    pub question: String,
    pub tokens: Vec<String>,
    pub prices: Vec<f64>,
    pub closed: bool,
    /// Unix seconds the game starts (sports markets), to tell pre-match from in-play.
    pub game_start: Option<f64>,
    /// Taker fee per share: rate * (p * (1 - p)) ^ exponent.
    pub fee_rate: f64,
    pub fee_exp: f64,
}

impl Market {
    pub fn price_of(&self, token: &str) -> Option<f64> {
        self.tokens.iter().position(|t| t == token).and_then(|i| self.prices.get(i).copied())
    }

    /// Settled: closed with every outcome priced at 0, 0.5 or 1 (a cancelled game pays half).
    pub fn settled(&self) -> bool {
        self.closed
            && !self.prices.is_empty()
            && self.prices.iter().all(|p| [0.0, 0.5, 1.0].iter().any(|v| (p - v).abs() < 1e-9))
    }

    pub fn fee_per_share(&self, p: f64) -> f64 {
        if self.fee_rate <= 0.0 { 0.0 } else { self.fee_rate * (p * (1.0 - p)).powf(self.fee_exp) }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Clone, Debug)]
pub struct Trade {
    pub wallet: String,
    pub side: Side,
    pub token: String,
    pub condition: String,
    pub size: f64,
    pub price: f64,
    /// Unix seconds of the wallet's trade.
    pub ts: f64,
    pub title: String,
    pub tx: String,
}

#[derive(Clone, Debug)]
pub struct WalletInfo {
    pub address: String,
    pub name: Option<String>,
}

#[derive(Clone)]
pub struct Api {
    http: reqwest::Client,
    markets: Arc<Mutex<HashMap<String, (f64, Market)>>>,
}

fn num(v: &Value) -> f64 {
    match v {
        Value::Number(n) => n.as_f64().unwrap_or(0.0),
        Value::String(s) => s.parse().unwrap_or(0.0),
        _ => 0.0,
    }
}

fn s(v: &Value) -> String {
    v.as_str().map(str::to_string).unwrap_or_default()
}

pub fn now() -> f64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64()
}

/// "2026-10-01 15:30:00+00" or "2026-10-01T15:30:00Z" -> unix seconds (UTC).
fn parse_utc(t: &str) -> Option<f64> {
    let t = t.get(..19)?.replace('T', " ");
    let (date, time) = t.split_once(' ')?;
    let d: Vec<i64> = date.split('-').map(|x| x.parse().ok()).collect::<Option<_>>()?;
    let h: Vec<i64> = time.split(':').map(|x| x.parse().ok()).collect::<Option<_>>()?;
    if d.len() != 3 || h.len() != 3 {
        return None;
    }
    // Days from the civil date (Howard Hinnant's algorithm).
    let (y, m, day) = (if d[1] <= 2 { d[0] - 1 } else { d[0] }, d[1], d[2]);
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    Some((days * 86400 + h[0] * 3600 + h[1] * 60 + h[2]) as f64)
}

impl Api {
    pub fn new() -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .user_agent("Mozilla/5.0 copytrader")
            .build()?;
        Ok(Self { http, markets: Arc::new(Mutex::new(HashMap::new())) })
    }

    /// GET JSON, retried on 429 / 5xx / network errors with backoff (the data API and the CLOB
    /// rate-limit per IP).
    async fn get(&self, url: &str) -> Result<Value> {
        let mut wait = 0.5;
        for attempt in 0..6 {
            match self.http.get(url).send().await {
                Ok(r) if r.status().is_success() => return Ok(r.json().await?),
                Ok(r) if r.status().as_u16() == 429 || r.status().is_server_error() => {}
                Ok(r) => bail!("GET {url}: {}", r.status()),
                Err(e) if attempt == 5 => return Err(e.into()),
                Err(_) => {}
            }
            tokio::time::sleep(Duration::from_secs_f64(wait)).await;
            wait *= 2.0;
        }
        bail!("GET {url}: still rate-limited")
    }

    pub async fn book(&self, token: &str) -> Result<Book> {
        let v = self.get(&format!("{CLOB}/book?token_id={token}")).await?;
        let side = |k: &str| -> Vec<(f64, f64)> {
            v[k].as_array().map(|a| a.iter().map(|l| (num(&l["price"]), num(&l["size"]))).collect()).unwrap_or_default()
        };
        let mut asks = side("asks");
        let mut bids = side("bids");
        asks.sort_by(|a, b| a.0.total_cmp(&b.0));
        bids.sort_by(|a, b| b.0.total_cmp(&a.0));
        let tick = match num(&v["tick_size"]) {
            t if t > 0.0 => t,
            _ => 0.01,
        };
        Ok(Book { asks, bids, tick })
    }

    fn parse_market(m: &Value) -> Option<Market> {
        let tokens: Vec<String> = serde_json::from_str(m["clobTokenIds"].as_str()?).ok()?;
        let prices: Vec<f64> = serde_json::from_str::<Vec<String>>(m["outcomePrices"].as_str().unwrap_or("[]"))
            .ok()?
            .iter()
            .map(|p| p.parse().unwrap_or(0.0))
            .collect();
        let game_start = m["gameStartTime"].as_str().or(m["eventStartTime"].as_str()).and_then(parse_utc);
        let fees = m["feesEnabled"].as_bool().unwrap_or(false);
        let sched = &m["feeSchedule"];
        Some(Market {
            condition: s(&m["conditionId"]),
            question: s(&m["question"]),
            tokens,
            prices,
            closed: m["closed"].as_bool().unwrap_or(false),
            game_start,
            fee_rate: if fees { sched.get("rate").map(num).unwrap_or(0.07) } else { 0.0 },
            fee_exp: sched.get("exponent").map(num).filter(|e| *e > 0.0).unwrap_or(1.0),
        })
    }

    /// The market a token belongs to, cached for `max_age_s` (gamma lists closed markets only
    /// with closed=true).
    pub async fn market(&self, token: &str, max_age_s: f64) -> Result<Market> {
        if let Some((at, m)) = self.markets.lock().await.get(token) {
            if now() - at < max_age_s {
                return Ok(m.clone());
            }
        }
        let mut found = None;
        for closed in ["", "&closed=true"] {
            let v = self.get(&format!("{GAMMA}/markets?clob_token_ids={token}{closed}")).await?;
            if let Some(m) = v.as_array().and_then(|a| a.first()).and_then(Self::parse_market) {
                found = Some(m);
                break;
            }
        }
        let m = found.with_context(|| format!("no market for token {token}"))?;
        let mut cache = self.markets.lock().await;
        for t in &m.tokens {
            cache.insert(t.clone(), (now(), m.clone()));
        }
        Ok(m)
    }

    /// What the wallet holds of the outcome now (not settled positions). The API lags about
    /// a minute behind trades.
    pub async fn position(&self, wallet: &str, token: &str) -> Result<f64> {
        let v = self.get(&format!("{DATA}/positions?user={wallet}&sizeThreshold=0&limit=500")).await?;
        Ok(v.as_array()
            .map(|a| {
                a.iter()
                    .filter(|p| s(&p["asset"]) == token && !p["redeemable"].as_bool().unwrap_or(false))
                    .map(|p| num(&p["size"]))
                    .sum()
            })
            .unwrap_or(0.0))
    }

    /// The wallet's trades since `since` (unix seconds), oldest first.
    pub async fn trades_since(&self, wallet: &str, since: f64) -> Result<Vec<Trade>> {
        let v = self
            .get(&format!("{DATA}/v2/trades?user={wallet}&start={}&limit=200&taker_only=false", since as i64))
            .await?;
        let mut out: Vec<Trade> = v["data"]
            .as_array()
            .map(|a| {
                a.iter()
                    .map(|r| Trade {
                        wallet: s(&r["proxy_wallet"]).to_lowercase(),
                        side: if s(&r["side"]).eq_ignore_ascii_case("BUY") { Side::Buy } else { Side::Sell },
                        token: s(&r["token_id"]),
                        condition: s(&r["condition_id"]),
                        size: num(&r["size"]),
                        price: num(&r["price"]),
                        ts: num(&r["timestamp"]),
                        title: s(&r["title"]),
                        tx: s(&r["transaction_hash"]),
                    })
                    .collect()
            })
            .unwrap_or_default();
        out.sort_by(|a, b| a.ts.total_cmp(&b.ts));
        Ok(out)
    }

    /// Every wallet on the Polyfox smart-wallet leaderboard.
    pub async fn polyfox_wallets(&self) -> Result<Vec<WalletInfo>> {
        let mut out = Vec::new();
        let mut off = 0;
        loop {
            let v = self.get(&format!("{POLYFOX}?sort=score-desc&offset={off}&limit=50")).await?;
            let rows = v["rows"].as_array().cloned().unwrap_or_default();
            for r in &rows {
                out.push(WalletInfo { address: s(&r["address"]).to_lowercase(), name: r["displayName"].as_str().map(str::to_string) });
            }
            off += 50;
            if rows.is_empty() || off as f64 >= num(&v["total"]) {
                return Ok(out);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_gamma_times() {
        assert_eq!(parse_utc("2026-10-01 15:30:00+00"), Some(1790868600.0));
        assert_eq!(parse_utc("2026-10-01T15:30:00Z"), Some(1790868600.0));
        assert_eq!(parse_utc("bad"), None);
    }

    #[test]
    fn settled_needs_final_prices() {
        let mut m = Market {
            condition: String::new(), question: String::new(), tokens: vec!["a".into(), "b".into()],
            prices: vec![1.0, 0.0], closed: true, game_start: None, fee_rate: 0.0, fee_exp: 1.0,
        };
        assert!(m.settled());
        m.prices = vec![0.5, 0.5];
        assert!(m.settled());
        m.prices = vec![0.62, 0.38];
        assert!(!m.settled());
        m.prices = vec![1.0, 0.0];
        m.closed = false;
        assert!(!m.settled());
    }
}
