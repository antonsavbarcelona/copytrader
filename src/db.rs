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
CREATE OR REPLACE VIEW buys AS
SELECT id, at, wallet, title, data->>'phase' AS phase, data->>'source' AS source,
       data->>'missed' AS missed, (data->>'wallet_price')::float8 AS wallet_price,
       (data->>'cap')::float8 AS cap, (data->'fill'->>'avg')::float8 AS fill_price,
       (data->'fill'->>'usd')::float8 AS usd, (data->'fill'->>'fee')::float8 AS fee,
       (data->'fill'->>'shares')::float8 AS shares, (data->>'vs_wallet_bps')::float8 AS vs_wallet_bps,
       (data->>'detect_s')::float8 AS detect_s
FROM events WHERE kind = 'buy';
"#;

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
    Event(Value),
    State(String),
    Wallets(Vec<(String, Option<String>)>),
}

async fn apply(c: &Client, w: &Write) -> Result<()> {
    match w {
        Write::Event(ev) => {
            let s = |k: &str| ev[k].as_str().map(str::to_string);
            c.execute(
                "INSERT INTO events (at, kind, wallet, token, condition, title, data)
                 VALUES (to_timestamp($1), $2, $3, $4, $5, $6, $7)",
                &[&ev["at"].as_f64().unwrap_or(0.0), &s("kind").unwrap_or_default(), &s("wallet"), &s("token"),
                  &s("condition"), &s("title"), ev],
            )
            .await?;
        }
        Write::State(json) => {
            c.execute(
                "INSERT INTO state (id, saved_at, data) VALUES ('main', now(), $1::text::jsonb)
                 ON CONFLICT (id) DO UPDATE SET saved_at = now(), data = EXCLUDED.data",
                &[json],
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

pub async fn load_state(c: &Client) -> Result<Option<String>> {
    Ok(c.query_opt("SELECT data::text FROM state WHERE id = 'main'", &[]).await?.map(|r| r.get(0)))
}

pub async fn load_events(c: &Client) -> Result<Vec<Value>> {
    Ok(c.query("SELECT data FROM events ORDER BY id", &[]).await?.iter().map(|r| r.get(0)).collect())
}
