//! Real orders on Polymarket's CLOB for the live copy, signed for an email (Magic) account:
//! the key is the account's exported private key, the funds sit on its proxy wallet
//! (signature type 1), derived from the key unless POLYMARKET_FUNDER says otherwise.
//!
//! A buy is a FAK market order capped at a price, a sale a FAK market sale floored at one.
//! A won outcome is redeemed once its market resolves, the way polymarket.com does it: the
//! proxy wallet calls Polymarket's collateral adapter (CTF redeem, paid out in pUSD), sent
//! through Polymarket's relayer, which pays the gas (no POL needed).
//!
//!     POLYMARKET_PRIVATE_KEY   the account's private key (0x...; PRIVATE_KEY also read)
//!     POLYMARKET_FUNDER        optional: the proxy wallet address shown on polymarket.com
//!     RELAYER_API_KEY, RELAYER_ADDRESS
//!                              the relayer's API key and the address it was made for
//!                              (polymarket.com, Settings -> Relayer API keys), or else
//!     POLY_BUILDER_API_KEY, POLY_BUILDER_SECRET, POLY_BUILDER_PASSPHRASE
//!                              builder keys (Settings -> Builder)
//!     POLYGON_RPC              optional: for the redeem's gas estimate

use std::str::FromStr;
use std::time::Duration;

use alloy::primitives::{B256, keccak256};
use alloy::signers::Signer as _;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol_types::SolCall as _;
use anyhow::{Context, Result, bail};
use base64::Engine as _;
use hmac::{Hmac, Mac};
use polymarket_client_sdk_v2::POLYGON;
use polymarket_client_sdk_v2::auth::Normal;
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::clob::types::request::{BalanceAllowanceRequest, TradesRequest};
use polymarket_client_sdk_v2::clob::types::response::PostOrderResponse;
use polymarket_client_sdk_v2::clob::types::{Amount, AssetType, OrderStatusType, OrderType, Side, SignatureType};
use polymarket_client_sdk_v2::clob::{Client, Config};
use polymarket_client_sdk_v2::types::{Address, Decimal, U256};

use crate::fill::Fill;

const HOST: &str = "https://clob.polymarket.com";
const RELAYER: &str = "https://relayer-v2.polymarket.com";
const DEFAULT_RPC: &str = "https://polygon.drpc.org";
/// Balances come back in the collateral's and the outcome tokens' base units (6 decimals).
const UNIT: f64 = 1e6;

// Polygon contracts (as polymarket.com's own redeems call them, seen on chain 2026-10-09).
const PROXY_FACTORY: &str = "0xaB45c5A4B0c941a2F231C04C3f49182e1A254052";
const RELAY_HUB: &str = "0xD216153c06E857cD7f72665E0aF1d7D82172F494";
const PUSD: &str = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB";
/// Redeem on the CTF and pay out in pUSD: plain markets ...
const REDEEM_ADAPTER: &str = "0xada100db00ca00073811820692005400218fce1f";
/// ... and neg-risk ones.
const REDEEM_ADAPTER_NEG_RISK: &str = "0xada2005600dec949baf300f4c6120000bdb6eaab";
/// Used when the gas estimate fails (its relayed redeems take ~400k).
const DEFAULT_GAS_LIMIT: u64 = 600_000;

alloy::sol! {
    struct ProxyCall { uint8 typeCode; address to; uint256 value; bytes data; }
    function proxy(ProxyCall[] calls);
    function redeemPositions(address collateralToken, bytes32 parentCollectionId, bytes32 conditionId, uint256[] indexSets);
}

pub struct Live {
    client: Client<Authenticated<Normal>>,
    signer: PrivateKeySigner,
    /// The proxy wallet holding the funds.
    pub funder: Address,
    http: reqwest::Client,
}

fn dec(x: f64) -> Result<Decimal> {
    Decimal::from_str(&format!("{x:.6}")).context("decimal")
}

fn f(d: Decimal) -> f64 {
    d.to_string().parse().unwrap_or(0.0)
}

impl Live {
    pub async fn connect() -> Result<Self> {
        let key = env("POLYMARKET_PRIVATE_KEY").or_else(|| env("PRIVATE_KEY")).context("POLYMARKET_PRIVATE_KEY not set")?;
        let signer = PrivateKeySigner::from_str(key.trim())?.with_chain_id(Some(POLYGON));
        let funder = match std::env::var("POLYMARKET_FUNDER").ok().filter(|s| !s.trim().is_empty()) {
            Some(f) => Address::from_str(f.trim())?,
            None => polymarket_client_sdk_v2::derive_proxy_wallet(signer.address(), POLYGON).context("proxy wallet")?,
        };
        let client = Client::new(HOST, Config::default())?
            .authentication_builder(&signer)
            .signature_type(SignatureType::Proxy)
            .funder(funder)
            .authenticate()
            .await?;
        let http = reqwest::Client::builder().timeout(Duration::from_secs(20)).build()?;
        Ok(Self { client, signer, funder, http })
    }

    pub fn signer_address(&self) -> Address {
        self.signer.address()
    }

    /// The collateral (pUSD) the account can spend, in dollars.
    pub async fn cash(&self) -> Result<f64> {
        let r = self.client.balance_allowance(BalanceAllowanceRequest::builder().asset_type(AssetType::Collateral).build()).await?;
        Ok(f(r.balance) / UNIT)
    }

    /// Shares of `token` the account holds.
    pub async fn shares(&self, token: &str) -> Result<f64> {
        let r = self
            .client
            .balance_allowance(
                BalanceAllowanceRequest::builder().asset_type(AssetType::Conditional).token_id(U256::from_str(token)?).build(),
            )
            .await?;
        Ok(f(r.balance) / UNIT)
    }

    /// `usd` at market, paying at most `cap` a share; what the book lacks there is cancelled.
    pub async fn buy(&self, token: &str, usd: f64, cap: f64) -> Result<(Fill, PostOrderResponse)> {
        let r = self
            .client
            .market_order()
            .token_id(U256::from_str(token)?)
            .side(Side::Buy)
            .amount(Amount::usdc(dec(usd)?)?)
            .price(dec(cap)?)
            .order_type(OrderType::FAK)
            .build_sign_and_post(&self.signer)
            .await?;
        check(&r)?;
        // A buy makes dollars and takes shares.
        self.settled(r, |r| Fill { shares: f(r.taking_amount), usd: f(r.making_amount), fee: 0.0 }).await
    }

    /// `shares` at market, taking no less than `floor` a share.
    pub async fn sell(&self, token: &str, shares: f64, floor: f64) -> Result<(Fill, PostOrderResponse)> {
        // Order sizes go in hundredths of a share: never round up past what is held.
        let shares = (shares * 100.0).floor() / 100.0;
        let r = self
            .client
            .market_order()
            .token_id(U256::from_str(token)?)
            .side(Side::Sell)
            .amount(Amount::shares(dec(shares)?)?)
            .price(dec(floor)?)
            .order_type(OrderType::FAK)
            .build_sign_and_post(&self.signer)
            .await?;
        check(&r)?;
        // A sale makes shares and takes dollars.
        self.settled(r, |r| Fill { shares: f(r.making_amount), usd: f(r.taking_amount), fee: 0.0 }).await
    }
}

impl Live {
    /// The order's fill. A taker order on an in-play sports market comes back `Delayed` (the
    /// exchange holds it a few seconds) with nothing matched yet: then its final state is read
    /// back, and the fill is its trades. `r.status` is set to the final state.
    async fn settled(&self, mut r: PostOrderResponse, now: impl Fn(&PostOrderResponse) -> Fill)
        -> Result<(Fill, PostOrderResponse)> {
        if !matches!(r.status, OrderStatusType::Delayed | OrderStatusType::Live) {
            return Ok((now(&r), r));
        }
        for _ in 0..30 {
            tokio::time::sleep(Duration::from_millis(1000)).await;
            let Ok(o) = self.client.order(&r.order_id).await else { continue };
            if matches!(o.status, OrderStatusType::Delayed | OrderStatusType::Live) {
                continue;
            }
            let shares = f(o.size_matched);
            // Dollars from its trades (each at its own price); the order's limit if unreadable.
            let mut usd = 0.0;
            let mut got = 0.0;
            for id in &o.associate_trades {
                if let Ok(p) = self.client.trades(&TradesRequest::builder().id(id.clone()).build(), None).await {
                    for t in p.data {
                        usd += f(t.size) * f(t.price);
                        got += f(t.size);
                    }
                }
            }
            if (got - shares).abs() > 1e-6 {
                usd = shares * f(o.price);
            }
            r.status = o.status;
            return Ok((Fill { shares, usd, fee: 0.0 }, r));
        }
        bail!("order {} still {:?} after 30 s", r.order_id, r.status)
    }

    /// The resolved markets the proxy wallet holds a winning outcome in (also any bought
    /// outside this run's records), as (condition, neg risk). From the public data API.
    pub async fn won_unredeemed(&self) -> Result<Vec<(String, bool, String)>> {
        let v: serde_json::Value = self
            .http
            .get(format!("https://data-api.polymarket.com/positions?user={}&sizeThreshold=0&limit=500", self.funder))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let mut out: Vec<(String, bool, String)> = v
            .as_array()
            .map(|a| {
                a.iter()
                    .filter(|p| p["redeemable"].as_bool() == Some(true) && p["curPrice"].as_f64().unwrap_or(0.0) > 0.0)
                    .filter_map(|p| {
                        Some((p["conditionId"].as_str()?.to_string(), p["negativeRisk"].as_bool().unwrap_or(false),
                            p["title"].as_str().unwrap_or("").to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default();
        out.sort();
        out.dedup_by(|a, b| a.0 == b.0);
        Ok(out)
    }

    /// Redeems every outcome of a resolved market the proxy wallet holds (winners pay out in
    /// pUSD, losers burn), through the relayer. Returns the transaction hash once mined.
    pub async fn redeem(&self, condition: &str, neg_risk: bool) -> Result<String> {
        let from = self.signer.address();
        let factory = Address::from_str(PROXY_FACTORY)?;
        let adapter = Address::from_str(if neg_risk { REDEEM_ADAPTER_NEG_RISK } else { REDEEM_ADAPTER })?;
        let redeem = redeemPositionsCall {
            collateralToken: Address::from_str(PUSD)?,
            parentCollectionId: B256::ZERO,
            conditionId: B256::from_str(condition)?,
            indexSets: vec![U256::from(1), U256::from(2)],
        };
        let data = proxyCall {
            calls: vec![ProxyCall { typeCode: 1, to: adapter, value: U256::ZERO, data: redeem.abi_encode().into() }],
        }
        .abi_encode();

        let rp: serde_json::Value =
            self.http.get(format!("{RELAYER}/relay-payload?address={from}&type=PROXY")).send().await?.error_for_status()?.json().await?;
        let relay = Address::from_str(rp["address"].as_str().context("relay-payload: no address")?)?;
        let nonce: u64 = match &rp["nonce"] {
            serde_json::Value::String(s) => s.parse()?,
            v => v.as_u64().context("relay-payload: no nonce")?,
        };
        let gas_limit = self.estimate_gas(from, factory, &data).await.unwrap_or(DEFAULT_GAS_LIMIT);
        let hub = Address::from_str(RELAY_HUB)?;
        // The relay hub's struct hash: "rlx:" from to data fee gasPrice gasLimit nonce hub relay.
        let mut msg = b"rlx:".to_vec();
        msg.extend_from_slice(from.as_slice());
        msg.extend_from_slice(factory.as_slice());
        msg.extend_from_slice(&data);
        for n in [0u64, 0, gas_limit, nonce] {
            msg.extend_from_slice(&U256::from(n).to_be_bytes::<32>());
        }
        msg.extend_from_slice(hub.as_slice());
        msg.extend_from_slice(relay.as_slice());
        let hash = keccak256(&msg);
        let sig = self.signer.sign_message(hash.as_slice()).await?;
        let body = serde_json::json!({
            "from": from.to_checksum(None),
            "to": factory.to_checksum(None),
            "proxyWallet": self.funder.to_checksum(None),
            "data": format!("0x{}", hex(&data)),
            "nonce": nonce.to_string(),
            "signature": format!("0x{}", hex(&sig.as_bytes())),
            "signatureParams": {
                "gasPrice": "0", "gasLimit": gas_limit.to_string(), "relayerFee": "0",
                "relayHub": hub.to_checksum(None), "relay": relay.to_checksum(None),
            },
            "type": "PROXY",
            "metadata": "redeem",
        })
        .to_string();
        let mut req = self.http.post(format!("{RELAYER}/submit")).header("content-type", "application/json");
        for (k, v) in relayer_headers("POST", "/submit", &body)? {
            req = req.header(k, v);
        }
        let resp = req.body(body).send().await?;
        let status = resp.status();
        let text = resp.text().await?;
        if !status.is_success() {
            bail!("relayer submit {status}: {text}");
        }
        let v: serde_json::Value = serde_json::from_str(&text)?;
        let id = v["transactionID"].as_str().context("relayer: no transactionID")?.to_string();
        // Mined within a few blocks; give it two minutes.
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_secs(3)).await;
            let r: serde_json::Value =
                self.http.get(format!("{RELAYER}/transaction?id={id}")).send().await?.json().await.unwrap_or_default();
            let t = r.as_array().and_then(|a| a.first()).cloned().unwrap_or_default();
            let state = t["state"].as_str().unwrap_or("");
            let tx = t["transactionHash"].as_str().unwrap_or("").to_string();
            match state {
                "STATE_MINED" | "STATE_CONFIRMED" => return Ok(tx),
                "STATE_FAILED" | "STATE_INVALID" => bail!("redeem {state} (tx {tx})"),
                _ => {}
            }
        }
        bail!("redeem {id}: not mined after 2 minutes")
    }

    async fn estimate_gas(&self, from: Address, to: Address, data: &[u8]) -> Option<u64> {
        let rpc = std::env::var("POLYGON_RPC").ok().filter(|s| !s.is_empty()).unwrap_or_else(|| DEFAULT_RPC.into());
        let body = serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "eth_estimateGas",
            "params": [{"from": from.to_string(), "to": to.to_string(), "data": format!("0x{}", hex(data))}]});
        let v: serde_json::Value = self.http.post(rpc).json(&body).send().await.ok()?.json().await.ok()?;
        let g = u64::from_str_radix(v["result"].as_str()?.trim_start_matches("0x"), 16).ok()?;
        // Headroom: the relay hub's own bookkeeping comes on top.
        Some(g * 13 / 10)
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
}

/// The relayer's authentication: a relayer API key (two headers, as Polymarket's own client
/// sends them), else builder keys: an HMAC of timestamp + method + path + body with the
/// builder secret (url-safe base64, padding kept). Neither set: no headers.
fn relayer_headers(method: &str, path: &str, body: &str) -> Result<Vec<(&'static str, String)>> {
    if let (Some(key), Some(addr)) = (env("RELAYER_API_KEY"), env("RELAYER_ADDRESS").or_else(|| env("RELAYER_API_KEY_ADDRESS"))) {
        return Ok(vec![("RELAYER_API_KEY", key), ("RELAYER_API_KEY_ADDRESS", addr)]);
    }
    let var = env;
    let (Some(key), Some(secret), Some(pass)) =
        (var("POLY_BUILDER_API_KEY"), var("POLY_BUILDER_SECRET"), var("POLY_BUILDER_PASSPHRASE"))
    else {
        return Ok(Vec::new());
    };
    let ts = crate::api::now() as u64;
    let b64 = base64::engine::general_purpose::STANDARD;
    let secret = b64.decode(secret.trim().replace('-', "+").replace('_', "/"))?;
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(&secret)?;
    mac.update(format!("{ts}{method}{path}{body}").as_bytes());
    let sig = b64.encode(mac.finalize().into_bytes()).replace('+', "-").replace('/', "_");
    Ok(vec![
        ("POLY_BUILDER_API_KEY", key),
        ("POLY_BUILDER_PASSPHRASE", pass),
        ("POLY_BUILDER_SIGNATURE", sig),
        ("POLY_BUILDER_TIMESTAMP", ts.to_string()),
    ])
}

fn check(r: &PostOrderResponse) -> Result<()> {
    if !r.success {
        bail!("order refused: {}", r.error_msg.clone().unwrap_or_default());
    }
    Ok(())
}
