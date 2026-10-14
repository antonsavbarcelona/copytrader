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
-- Round one (scout.rs): every wallet judged on its record, and whether it passed.
CREATE TABLE IF NOT EXISTS scouted (
    wallet    text PRIMARY KEY,
    name      text,
    judged_at timestamptz NOT NULL DEFAULT now(),
    ok        boolean NOT NULL,
    stats     jsonb
);
-- Round two (trial.rs): the weeks on paper. status trial | review | golden | rotated; the
-- week under way started at week_start; strikes = weeks failed in a row; weeks = every
-- week's verdict, last = the latest.
CREATE TABLE IF NOT EXISTS trials (
    wallet       text PRIMARY KEY,
    name         text,
    status       text NOT NULL,
    since        timestamptz NOT NULL DEFAULT now(),
    week         int NOT NULL DEFAULT 1,
    week_start   timestamptz NOT NULL,
    strikes      int NOT NULL DEFAULT 0,
    updated      timestamptz NOT NULL DEFAULT now(),
    golden_since timestamptz,
    scout        jsonb,
    last         jsonb,
    weeks        jsonb NOT NULL DEFAULT '[]'
);
-- The allocator before its generations (138 books, 2026-10-12/13): its tables are gone.
DROP TABLE IF EXISTS alloc, alloc_rank, alloc_snap;
-- The allocator's generations (allocator.rs): every closed position of a book, the entries
-- cut or refused, every book's status each hour.
CREATE TABLE IF NOT EXISTS alloc_trades (
    gen       int NOT NULL,
    book      text NOT NULL,
    entry_id  bigint NOT NULL,
    close_id  bigint NOT NULL,
    wallet    text NOT NULL,
    token     text NOT NULL,
    market    text NOT NULL,
    event     text NOT NULL,
    opened_at timestamptz NOT NULL,
    closed_at timestamptz NOT NULL,
    price     float8 NOT NULL,
    capital   float8 NOT NULL,
    stake     float8 NOT NULL,
    shares    float8 NOT NULL,
    back      float8 NOT NULL,
    pnl       float8 NOT NULL,
    r         float8 NOT NULL,
    hold_h    float8 NOT NULL,
    reason    text NOT NULL,
    PRIMARY KEY (gen, book, entry_id)
);
CREATE TABLE IF NOT EXISTS alloc_decisions (
    gen      int NOT NULL,
    at       timestamptz NOT NULL,
    book     text NOT NULL,
    event_id bigint NOT NULL,
    wallet   text NOT NULL,
    token    text NOT NULL,
    market   text NOT NULL,
    event    text NOT NULL,
    wanted   float8 NOT NULL,
    granted  float8 NOT NULL,
    reason   text NOT NULL,
    PRIMARY KEY (gen, book, event_id)
);
CREATE TABLE IF NOT EXISTS alloc_status (
    gen       int NOT NULL,
    at        timestamptz NOT NULL,
    last_id   bigint NOT NULL,
    book      text NOT NULL,
    capital   float8 NOT NULL,
    equity    float8 NOT NULL,
    open_risk float8 NOT NULL,
    event_max float8 NOT NULL,
    positions bigint NOT NULL,
    entries   bigint NOT NULL,
    cuts      bigint NOT NULL,
    refusals  bigint NOT NULL,
    closed    bigint NOT NULL,
    PRIMARY KEY (gen, at, book)
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

/// The events of one table, one statement.
async fn insert_events(c: &Client, evs: &[&Value], live: bool) -> Result<()> {
    let s = |k: &str| evs.iter().map(|ev| ev[k].as_str().map(str::to_string)).collect::<Vec<Option<String>>>();
    let at: Vec<f64> = evs.iter().map(|ev| ev["at"].as_f64().unwrap_or(0.0)).collect();
    let kind: Vec<String> = s("kind").into_iter().map(Option::unwrap_or_default).collect();
    let data: Vec<Value> = evs.iter().map(|ev| (*ev).clone()).collect();
    c.execute(
        &format!("INSERT INTO {} (at, kind, wallet, token, condition, title, data)
                  SELECT to_timestamp(a), k, w, t, c, ti, d
                  FROM unnest($1::float8[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::jsonb[])
                       WITH ORDINALITY AS u(a, k, w, t, c, ti, d, o) ORDER BY o", events_table(live)),
        &[&at, &kind, &s("wallet"), &s("token"), &s("condition"), &s("title"), &data],
    )
    .await?;
    Ok(())
}

/// What is queued, applied in order: the events of a run one statement a table; of the state
/// snapshots of one copy only the last (each replaces the one before).
async fn apply_all(c: &Client, ws: &[Write]) -> Result<()> {
    let last_state: Vec<usize> = [false, true]
        .iter()
        .filter_map(|l| ws.iter().rposition(|w| matches!(w, Write::State(_, live) if live == l)))
        .collect();
    let mut i = 0;
    while i < ws.len() {
        match &ws[i] {
            Write::Event(_, live) => {
                let mut evs = Vec::new();
                while let Some(Write::Event(ev, l)) = ws.get(i) {
                    if l != live || evs.len() >= 2000 {
                        break;
                    }
                    evs.push(ev);
                    i += 1;
                }
                insert_events(c, &evs, *live).await?;
                continue;
            }
            Write::State(..) if !last_state.contains(&i) => {}
            w => apply(c, w).await?,
        }
        i += 1;
    }
    Ok(())
}

/// Applies the writes in order, all that are queued at once; on an error reconnects and
/// retries them. Commits do not wait for the disk (synchronous_commit off): a crash of the
/// database loses at most its last fraction of a second, a write a row at a time fell behind.
pub async fn writer(url: String, mut client: Option<Client>, mut rx: mpsc::UnboundedReceiver<Write>) {
    let mut fast = false;
    while let Some(first) = rx.recv().await {
        let mut ws = vec![first];
        while ws.len() < 20_000 {
            match rx.try_recv() {
                Ok(w) => ws.push(w),
                Err(_) => break,
            }
        }
        let mut wait = 1.0;
        loop {
            if client.as_ref().is_none_or(|c| c.is_closed()) {
                fast = false;
                client = match connect(&url).await {
                    Ok(c) => Some(c),
                    Err(e) => {
                        log!("db: connect failed: {e}");
                        None
                    }
                };
            }
            if let Some(c) = &client {
                if !fast {
                    fast = c.batch_execute("SET synchronous_commit = off").await.is_ok();
                }
                match apply_all(c, &ws).await {
                    Ok(()) => break,
                    Err(e) => {
                        log!("db: write of {} failed: {e}", ws.len());
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
