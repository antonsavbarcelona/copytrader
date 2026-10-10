//! Round two: paper. Every big player from round one (scout.rs) is copied on paper and,
//! once a week old, checked every day on its last 7 days (not calendar weeks): the copies of
//! the trades it opened in them (sold, settled, or still held at the market's price now), its
//! own profit next to them.
//!
//! - Copies in profit, on `MIN_BUYS` or more: the copy worked. Then the wallet's record
//!   (metric B, scout::record, read again now) tells steady from lucky: steady goes on the
//!   golden list, which the live copy draws from; lucky is dropped (`rotated`).
//! - Otherwise it stays on trial; 14 days without a check that worked put it up for review
//!   (`review`: look at what it does), 21 rotate it out (no longer followed).
//!
//! A golden wallet stays on paper and is checked daily as well: its last 7 days in loss and
//! the 7 before them too send it back to trial (a new one from that day).
//!
//! The state is the `trials` table (db.rs): `week_start` is when the trial started, `week`
//! the week of it now; the last 60 checks are kept in `weeks`.

use anyhow::Result;
use serde_json::{Value, json};
use tokio_postgres::Client;

use crate::api::Api;
use crate::log;
use crate::scout::{self, Criteria, Judged, at};

const WEEK: f64 = 7.0 * 86400.0;
/// The checks kept per wallet.
const KEEP: i32 = 60;
/// Fewer copies than this in a week tell nothing.
const MIN_BUYS: i64 = 3;

/// Round one's results: every wallet judged goes to `scouted`; those that passed start their
/// trial. One already copied on paper starts it back to a week ago (or its first copy), so
/// the copies made count at once.
pub async fn enroll(c: &Client, judged: &[Judged]) -> Result<usize> {
    let mut new = 0;
    for j in judged {
        c.execute(
            "INSERT INTO scouted (wallet, name, ok, stats) VALUES ($1, $2, $3, $4)
             ON CONFLICT (wallet) DO UPDATE SET name = EXCLUDED.name, ok = EXCLUDED.ok, stats = EXCLUDED.stats,
               judged_at = now()",
            &[&j.wallet, &j.name, &j.ok, &j.stats],
        )
        .await?;
        if j.ok {
            new += c
                .execute(
                    "INSERT INTO trials (wallet, name, status, week_start, scout)
                     VALUES ($1, $2, 'trial', greatest(now() - interval '7 days',
                       least(now(), (SELECT min(at) FROM events WHERE wallet = $1 AND kind = 'buy'))), $3)
                     ON CONFLICT (wallet) DO NOTHING",
                    &[&j.wallet, &j.name, &j.stats],
                )
                .await? as usize;
        }
    }
    Ok(new)
}

/// The wallets round one should leave alone: in a trial (or rotated out), or judged in the
/// last `days`.
pub async fn known(c: &Client, days: f64) -> Result<Vec<String>> {
    Ok(c.query(
        "SELECT wallet FROM trials UNION SELECT wallet FROM scouted WHERE judged_at > now() - make_interval(secs => $1)",
        &[&(days * 86400.0)],
    )
    .await?
    .iter()
    .map(|r| r.get(0))
    .collect())
}

/// The wallets copied on paper: on trial, up for review, or golden.
pub async fn followed(c: &Client) -> Result<Vec<String>> {
    Ok(c.query("SELECT wallet FROM trials WHERE status <> 'rotated'", &[]).await?.iter().map(|r| r.get(0)).collect())
}

/// The golden list, the copies' return over their last 7 days first: (wallet, name, return), up to
/// `n`.
pub async fn golden(c: &Client, n: i64) -> Result<Vec<(String, Option<String>, f64)>> {
    Ok(c.query(
        "SELECT wallet, name, coalesce((last->>'copy_roi')::float8, 0) AS roi FROM trials WHERE status = 'golden'
         ORDER BY roi DESC LIMIT $1",
        &[&n],
    )
    .await?
    .iter()
    .map(|r| (r.get(0), r.get(1), r.get(2)))
    .collect())
}

/// The paper copies of the positions the wallet first bought in [from, to): (token, cost,
/// back, shares left, buys).
async fn copies(c: &Client, wallet: &str, from: f64, to: f64) -> Result<Vec<(String, f64, f64, f64, i64)>> {
    let rows = c
        .query(
            "SELECT token,
               coalesce(sum(CASE WHEN kind = 'buy' THEN (data->'fill'->>'usd')::float8 + coalesce((data->'fill'->>'fee')::float8, 0) END), 0),
               coalesce(sum(CASE WHEN kind = 'sell' THEN (data->'fill'->>'usd')::float8 - coalesce((data->'fill'->>'fee')::float8, 0)
                                 WHEN kind = 'settle' THEN (data->>'usd')::float8 END), 0),
               coalesce(sum(CASE WHEN kind = 'buy' THEN (data->'fill'->>'shares')::float8 END), 0)
                 - coalesce(sum(CASE WHEN kind = 'sell' THEN (data->'fill'->>'shares')::float8
                                     WHEN kind = 'settle' THEN (data->>'shares')::float8 END), 0),
               count(*) FILTER (WHERE kind = 'buy' AND (data->'fill'->>'shares')::float8 > 0)
             FROM events
             WHERE wallet = $1 AND kind IN ('buy', 'sell', 'settle') AND token IS NOT NULL
             GROUP BY token
             HAVING min(at) FILTER (WHERE kind = 'buy') >= to_timestamp($2)
                AND min(at) FILTER (WHERE kind = 'buy') < to_timestamp($3)",
            &[&wallet, &from, &to],
        )
        .await?;
    Ok(rows.iter().map(|r| (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4))).collect())
}

/// One week's verdict on the wallet: the copies' profit (open ones at the market's price
/// now) and the wallet's own.
async fn week_result(c: &Client, api: &Api, wallet: &str, from: f64) -> Result<Value> {
    let to = from + WEEK;
    let (mut cost, mut pnl, mut open, mut buys) = (0.0, 0.0, 0.0, 0);
    for (token, k, back, left, n) in copies(c, wallet, from, to).await? {
        cost += k;
        pnl += back - k;
        buys += n;
        if left > 1e-6 {
            let price = api.market(&token, 600.0).await?.price_of(&token).unwrap_or(0.0);
            open += left * price;
        }
    }
    pnl += open;
    let h = api.pnl_history(wallet, "1m", "1h").await?;
    let theirs = at(&h, to) - at(&h, from);
    let roi = if cost > 0.0 { pnl / cost } else { 0.0 };
    let r2 = |x: f64| (x * 100.0).round() / 100.0;
    Ok(json!({
        "from": from.round(), "to": to.round(), "buys": buys, "cost": r2(cost), "open_value": r2(open),
        "copy_pnl": r2(pnl), "copy_roi": (roi * 1000.0).round() / 1000.0, "trader_pnl": theirs.round(),
    }))
}

/// Whether the week's copies worked: in profit, on enough of them (None: too few to tell).
fn copies_worked(w: &Value) -> Option<bool> {
    (w["buys"].as_i64().unwrap_or(0) >= MIN_BUYS).then(|| w["copy_pnl"].as_f64().unwrap_or(0.0) > 0.0)
}

/// The status after a check: the last 7 days (`w`), the 7 before them (`prev`, golden only)
/// and how many days the trial has run; `steady` is the record's verdict, read only when the
/// copies worked (Err: why it looks like luck). Returns (status, failed weeks, verdict).
fn next(status: &str, days: f64, w: &Value, prev: Option<&Value>, steady: Result<(), &str>) -> (&'static str, i32, &'static str) {
    let worked = copies_worked(w);
    if status == "golden" {
        let before = prev.and_then(copies_worked);
        return match worked {
            None => ("golden", 0, "idle"),
            Some(true) => ("golden", 0, "pass"),
            Some(false) if before == Some(false) => ("trial", 0, "fail"),
            Some(false) => ("golden", 1, "fail"),
        };
    }
    let verdict = match worked {
        Some(true) if steady.is_ok() => return ("golden", 0, "pass"),
        Some(true) => return ("rotated", 0, "luck"),
        Some(false) => "fail",
        None => "idle",
    };
    let weeks = (days / 7.0).floor() as i32;
    match weeks {
        ..=1 => ("trial", weeks.max(1), verdict),
        2 => ("review", 2, verdict),
        n => ("rotated", n, verdict),
    }
}

/// Every wallet on trial a week or more (and every golden one) is checked on its last 7 days;
/// returns how many were checked.
pub async fn evaluate(c: &Client, api: &Api, crit: &Criteria) -> Result<usize> {
    let due = c
        .query(
            "SELECT wallet, name, status, extract(epoch FROM week_start)::float8 FROM trials
             WHERE status <> 'rotated' AND week_start + interval '7 days' <= now() ORDER BY week_start",
            &[],
        )
        .await?;
    let mut done = 0;
    for r in due {
        let (wallet, name, status, start): (String, Option<String>, String, f64) = (r.get(0), r.get(1), r.get(2), r.get(3));
        let t = crate::api::now();
        let days = (t - start) / 86400.0;
        let read = async {
            let w = week_result(c, api, &wallet, t - WEEK).await?;
            let prev = if status == "golden" { Some(week_result(c, api, &wallet, t - 2.0 * WEEK).await?) } else { None };
            anyhow::Ok((w, prev))
        };
        let (mut w, prev) = match read.await {
            Ok(x) => x,
            Err(e) => {
                log!("trial: {wallet} check failed, tomorrow again: {e}");
                continue;
            }
        };
        // The record matters once the copies worked: steady or lucky.
        let mut steady = Ok(());
        if status != "golden" && copies_worked(&w) == Some(true) {
            match scout::record_of(api, &wallet, crit).await {
                Ok(b) => {
                    steady = scout::steady(&b, crit);
                    w["record"] = b;
                }
                Err(e) => {
                    log!("trial: {wallet} record read failed, tomorrow again: {e}");
                    continue;
                }
            }
        }
        let (st, strikes, verdict) = next(&status, days, &w, prev.as_ref(), steady);
        if verdict == "luck" {
            w["luck"] = json!(steady.err().unwrap_or(""));
        }
        w["day"] = json!(days.floor());
        w["verdict"] = json!(verdict);
        w["status"] = json!(st);
        if st != status {
            log!("trial: {} day {:.0} {verdict}: {status} -> {st}, copies {:+.2} on {} buys in 7 days, the wallet {:+.0}",
                name.as_deref().unwrap_or(&wallet), days.floor(), w["copy_pnl"].as_f64().unwrap_or(0.0), w["buys"],
                w["trader_pnl"].as_f64().unwrap_or(0.0));
        }
        // Back from golden: a new trial from today.
        let restart = status == "golden" && st == "trial";
        c.execute(
            "UPDATE trials SET status = $2, strikes = $3, updated = now(), last = $4,
               week_start = CASE WHEN $5 THEN now() ELSE week_start END,
               week = CASE WHEN $5 THEN 1 ELSE floor(extract(epoch FROM now() - week_start) / 604800)::int + 1 END,
               weeks = jsonb_path_query_array(weeks || jsonb_build_array($4::jsonb), '$[last - $n + 1 to last]',
                                              jsonb_build_object('n', $6::int)),
               golden_since = CASE WHEN $2 = 'golden' AND status <> 'golden' THEN now()
                                   WHEN $2 = 'golden' THEN golden_since END
             WHERE wallet = $1",
            &[&wallet, &st, &strikes, &w, &restart, &KEEP],
        )
        .await?;
        done += 1;
    }
    Ok(done)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wk(buys: i64, copy: f64, theirs: f64) -> Value {
        json!({"buys": buys, "copy_pnl": copy, "trader_pnl": theirs})
    }

    #[test]
    fn checks_move_the_status() {
        let ok = Ok(());
        // copies in profit: steady -> golden, lucky -> out
        assert_eq!(next("trial", 7.0, &wk(10, 2.0, 500.0), None, ok), ("golden", 0, "pass"));
        assert_eq!(next("trial", 9.0, &wk(10, 2.0, 500.0), None, Err("one lucky day")), ("rotated", 0, "luck"));
        // copies in loss, or too few: on trial to day 14, review to 21, then out
        assert_eq!(next("trial", 7.0, &wk(10, -1.0, 500.0), None, ok), ("trial", 1, "fail"));
        assert_eq!(next("trial", 13.9, &wk(1, 3.0, 0.0), None, ok), ("trial", 1, "idle"));
        assert_eq!(next("trial", 14.0, &wk(10, -1.0, -500.0), None, ok), ("review", 2, "fail"));
        assert_eq!(next("review", 18.0, &wk(5, 1.0, -10.0), None, ok), ("golden", 0, "pass"));
        assert_eq!(next("review", 21.0, &wk(1, 3.0, 0.0), None, ok), ("rotated", 3, "idle"));
        // golden: 7 days in loss are tolerated, 14 send it back
        let lost = wk(10, -1.0, 500.0);
        assert_eq!(next("golden", 30.0, &lost, Some(&wk(10, 1.0, 0.0)), ok), ("golden", 1, "fail"));
        assert_eq!(next("golden", 30.0, &lost, Some(&lost), ok), ("trial", 0, "fail"));
        assert_eq!(next("golden", 30.0, &wk(0, 0.0, 0.0), Some(&lost), ok), ("golden", 0, "idle"));
        assert_eq!(next("golden", 30.0, &wk(10, 1.0, -5.0), Some(&lost), ok), ("golden", 0, "pass"));
    }
}
