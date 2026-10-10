//! Postgres (DATABASE_URL): every event, the state snapshot (so a restart or redeploy picks
//! up the open lots) and the followed wallets. Writes go through one task that retries until
//! the database answers, so a database outage never stalls the copier.

use std::time::Duration;

use anyhow::Result;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_postgres::{Client, NoTls};

use crate::log;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS events (
    id        bigserial PRIMARY KEY,
    at        timestamptz NOT NULL,
    kind      text NOT NULL,
    wallet    text,
    token     text,
    condition text,
    title     text,
    data      jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS events_kind_at ON events (kind, at);
CREATE INDEX IF NOT EXISTS events_wallet ON events (wallet);
CREATE TABLE IF NOT EXISTS state (
    id       text PRIMARY KEY,
    saved_at timestamptz NOT NULL,
    data     jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS wallets (
    wallet     text PRIMARY KEY,
    name       text,
    first_seen timestamptz NOT NULL DEFAULT now(),
    last_seen  timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS live_events (LIKE events INCLUDING ALL);
-- source: NULL for Polyfox wallets, 'crypto' for those found in the crypto markets
-- (discover.rs), with the record that got them in (stats).
ALTER TABLE wallets ADD COLUMN IF NOT EXISTS source text;
ALTER TABLE wallets ADD COLUMN IF NOT EXISTS stats jsonb;
-- The golden list: wallets whose paper copies earned over their trial (live copies the best
-- of these). active = in it now; since = when it last got in.
CREATE TABLE IF NOT EXISTS golden (
    wallet  text PRIMARY KEY,
    since   timestamptz NOT NULL DEFAULT now(),
    updated timestamptz NOT NULL DEFAULT now(),
    active  boolean NOT NULL DEFAULT true,
    roi     float8,
    closed  int,
    pnl     float8
);
CREATE OR REPLACE VIEW buys AS
SELECT id, at, wallet, title, data->>'phase' AS phase, data->>'source' AS source,
       data->>'missed' AS missed, (data->>'wallet_price')::float8 AS wallet_price,
       (data->>'cap')::float8 AS cap, (data->'fill'->>'avg')::float8 AS fill_price,
       (data->'fill'->>'usd')::float8 AS usd, (data->'fill'->>'fee')::float8 AS fee,
       (data->'fill'->>'shares')::float8 AS shares, (data->>'vs_wallet_bps')::float8 AS vs_wallet_bps,
       (data->>'detect_s')::float8 AS detect_s
FROM events WHERE kind = 'buy';
"#;

/// The live copy keeps its events in `live_events` and its state under id 'live'; the paper
/// copy in `events` and 'main'.
fn events_table(live: bool) -> &'static str {
    if live { "live_events" } else { "events" }
}

fn state_id(live: bool) -> &'static str {
    if live { "live" } else { "main" }
}

pub async fn connect(url: &str) -> Result<Client> {
    let (client, conn) = tokio_postgres::connect(url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            log!("db: connection lost: {e}");
        }
    });
    client.batch_execute(SCHEMA).await?;
    Ok(client)
}

pub enum Write {
    /// An event, of the live copy (true) or the paper one.
    Event(Value, bool),
    /// A state snapshot, of the live copy (true) or the paper one.
    State(String, bool),
    Wallets(Vec<(String, Option<String>)>),
    /// Wallets found in the crypto markets: (wallet, name, record).
    Discovered(Vec<(String, Option<String>, Value)>),
}

async fn apply(c: &Client, w: &Write) -> Result<()> {
    match w {
        Write::Event(ev, live) => {
            let s = |k: &str| ev[k].as_str().map(str::to_string);
            c.execute(
                &format!("INSERT INTO {} (at, kind, wallet, token, condition, title, data)
                 VALUES (to_timestamp($1), $2, $3, $4, $5, $6, $7)", events_table(*live)),
                &[&ev["at"].as_f64().unwrap_or(0.0), &s("kind").unwrap_or_default(), &s("wallet"), &s("token"),
                  &s("condition"), &s("title"), ev],
            )
            .await?;
        }
        Write::State(json, live) => {
            c.execute(
                "INSERT INTO state (id, saved_at, data) VALUES ($1, now(), $2::text::jsonb)
                 ON CONFLICT (id) DO UPDATE SET saved_at = now(), data = EXCLUDED.data",
                &[&state_id(*live), json],
            )
            .await?;
        }
        Write::Discovered(list) => {
            for (w, name, stats) in list {
                c.execute(
                    "INSERT INTO wallets (wallet, name, source, stats) VALUES ($1, $2, 'crypto', $3)
                     ON CONFLICT (wallet) DO UPDATE SET stats = EXCLUDED.stats, last_seen = now(),
                       source = coalesce(wallets.source, EXCLUDED.source)",
                    &[w, name, stats],
                )
                .await?;
            }
        }
        Write::Wallets(list) => {
            for (w, name) in list {
                c.execute(
                    "INSERT INTO wallets (wallet, name) VALUES ($1, $2)
                     ON CONFLICT (wallet) DO UPDATE SET name = EXCLUDED.name, last_seen = now()",
                    &[w, name],
                )
                .await?;
            }
        }
    }
    Ok(())
}

/// Applies the writes in order; on an error reconnects and retries the same write.
pub async fn writer(url: String, mut client: Option<Client>, mut rx: mpsc::UnboundedReceiver<Write>) {
    while let Some(w) = rx.recv().await {
        let mut wait = 1.0;
        loop {
            if client.as_ref().is_none_or(|c| c.is_closed()) {
                client = match connect(&url).await {
                    Ok(c) => Some(c),
                    Err(e) => {
                        log!("db: connect failed: {e}");
                        None
                    }
                };
            }
            if let Some(c) = &client {
                match apply(c, &w).await {
                    Ok(()) => break,
                    Err(e) => {
                        log!("db: write failed: {e}");
                        client = None;
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs_f64(wait)).await;
            wait = (wait * 2.0).min(60.0);
        }
    }
}

pub async fn load_state(c: &Client, live: bool) -> Result<Option<String>> {
    Ok(c.query_opt("SELECT data::text FROM state WHERE id = $1", &[&state_id(live)]).await?.map(|r| r.get(0)))
}

pub async fn load_events(c: &Client, live: bool) -> Result<Vec<Value>> {
    let q = format!("SELECT data FROM {} ORDER BY id", events_table(live));
    Ok(c.query(&q, &[]).await?.iter().map(|r| r.get(0)).collect())
}

/// The wallets found in the crypto markets (followed on paper from then on).
pub async fn crypto_wallets(c: &Client) -> Result<Vec<String>> {
    Ok(c.query("SELECT wallet FROM wallets WHERE source = 'crypto'", &[]).await?.iter().map(|r| r.get(0)).collect())
}

/// Each wallet's paper copies over the positions first bought in the last `secs`: the closed
/// ones' count, profit and return on cost. Wallets with no closed copy are left out.
const COPY_RESULTS: &str = "
    WITH k AS (
      SELECT wallet, token,
        sum(CASE WHEN kind = 'buy' THEN (data->'fill'->>'usd')::float8 + coalesce((data->'fill'->>'fee')::float8, 0) ELSE 0 END) AS cost,
        sum(CASE WHEN kind = 'sell' THEN (data->'fill'->>'usd')::float8 - coalesce((data->'fill'->>'fee')::float8, 0)
                 WHEN kind = 'settle' THEN (data->>'usd')::float8 ELSE 0 END) AS back,
        sum(CASE WHEN kind = 'buy' THEN (data->'fill'->>'shares')::float8 ELSE 0 END)
          - sum(CASE WHEN kind = 'sell' THEN (data->'fill'->>'shares')::float8
                     WHEN kind = 'settle' THEN (data->>'shares')::float8 ELSE 0 END) AS left_
      FROM events
      WHERE kind IN ('buy', 'sell', 'settle') AND wallet IS NOT NULL
      GROUP BY wallet, token
      HAVING min(at) FILTER (WHERE kind = 'buy') >= now() - make_interval(secs => $1))
    SELECT wallet, count(*) AS closed, sum(back - cost) AS pnl, sum(back - cost) / sum(cost) AS roi
    FROM k WHERE cost > 0 AND left_ <= 1e-6 GROUP BY wallet";

/// The golden list, worked out again and saved to `golden`: wallets followed on paper for at
/// least `days`, whose copies of the last `days` closed `min_closed` times or more with a
/// profit. Returns (wallet, name, roi, closed), best return first.
pub async fn golden_wallets(c: &Client, days: f64, min_closed: i64) -> Result<Vec<(String, Option<String>, f64, i64)>> {
    let q = format!(
        "WITH r AS ({COPY_RESULTS})
         SELECT r.wallet, w.name, r.roi, r.closed, r.pnl
         FROM r JOIN wallets w ON w.wallet = r.wallet
         WHERE w.first_seen <= now() - make_interval(secs => $1) AND r.closed >= $2 AND r.pnl > 0
         ORDER BY r.roi DESC"
    );
    let rows = c.query(&q, &[&(days * 86400.0), &min_closed]).await?;
    let list: Vec<(String, Option<String>, f64, i64, f64)> =
        rows.iter().map(|r| (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4))).collect();
    c.execute("UPDATE golden SET active = false WHERE active", &[]).await?;
    for (w, _, roi, closed, pnl) in &list {
        let closed = *closed as i32;
        c.execute(
            "INSERT INTO golden (wallet, roi, closed, pnl) VALUES ($1, $2, $3, $4)
             ON CONFLICT (wallet) DO UPDATE SET updated = now(), roi = EXCLUDED.roi, closed = EXCLUDED.closed,
               pnl = EXCLUDED.pnl, active = true,
               since = CASE WHEN golden.active THEN golden.since ELSE now() END",
            &[w, roi, &closed, pnl],
        )
        .await?;
    }
    Ok(list.into_iter().map(|(w, n, roi, closed, _)| (w, n, roi, closed)).collect())
}

/// Among `among` (the golden list), the wallets whose paper copies did best lately:
/// positions first bought in the last `days` and closed since, at least `min_closed` of them,
/// with a profit; the best `n` by return, with their name.
pub async fn top_wallets(c: &Client, days: f64, min_closed: i64, n: i64, among: &[String])
    -> Result<Vec<(String, Option<String>, f64, i64)>> {
    let q = format!(
        "WITH r AS ({COPY_RESULTS})
         SELECT r.wallet, w.name, r.roi, r.closed
         FROM r LEFT JOIN wallets w ON w.wallet = r.wallet
         WHERE r.closed >= $2 AND r.pnl > 0 AND r.wallet = ANY($4)
         ORDER BY r.roi DESC LIMIT $3"
    );
    let rows = c.query(&q, &[&(days * 86400.0), &min_closed, &n, &among]).await?;
    Ok(rows.iter().map(|r| (r.get(0), r.get(1), r.get(2), r.get(3))).collect())
}
