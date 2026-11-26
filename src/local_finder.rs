//! Local BIN sniper ("LOCAL_FINDER").
//!
//! Pipeline, once per Hypixel auction-house refresh (~60 s):
//! 1. Pull every page of `api.hypixel.net/v2/skyblock/auctions` and keep the
//!    BIN auctions that were listed since the previous refresh.
//! 2. Pre-filter them cheaply with the SkyAH bulk price file
//!    (`api.skyah.net/v1/prices.json`, one request every 5 min): anything
//!    clearly below the item's market price becomes a candidate.
//! 3. Value each candidate's exact item (stars, recomb, enchants, gems, pet
//!    level...) with Coflnet `POST /api/price/nbt`, authenticated with your
//!    SkyCofl premium token when one is configured.
//! 4. Candidates that still clear the profit thresholds after AH tax are sent
//!    into the normal flip pipeline as `CoflEvent::AuctionFlip`, so dedupe,
//!    buying, listing at `target` and webhooks all work unchanged.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Cursor, Read};
use std::time::{Duration, Instant};

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, info, warn};

use crate::types::Flip;
use crate::websocket::CoflEvent;

const HYPIXEL_AH: &str = "https://api.hypixel.net/v2/skyblock/auctions";
const SKYAH_PRICES: &str = "https://api.skyah.net/v1/prices.json";
const COFL_PRICE_NBT: &str = "https://sky.coflnet.com/api/price/nbt";
const USER_AGENT: &str = "FrikadellenBAF-local-finder";
pub const FINDER_NAME: &str = "LOCAL_FINDER";

/// `[local_finder]` section of config.toml.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LocalFinderConfig {
    /// Turn the local finder on.
    pub enabled: bool,
    /// Minimum profit in coins, after AH listing and claim tax.
    pub min_profit: u64,
    /// Minimum profit in percent of the buy price.
    pub min_profit_percent: f64,
    /// Never emit flips that cost more than this.
    pub max_price: u64,
    /// Minimum 24h sales on SkyAH for the item (or pet variant).
    pub min_daily_sales: u64,
    /// Minimum 24h sales volume Coflnet reports for the exact item.
    pub min_cofl_volume: f64,
    /// Pre-filter: listings must be at most this fraction of the SkyAH
    /// reference price to be priced by Coflnet (0.9 = 10% under market).
    pub prefilter_ratio: f64,
    /// Max Coflnet price lookups per AH refresh (keeps under rate limits).
    pub max_cofl_lookups: usize,
    /// Coflnet API token (SkyCofl premium). Sent as `Authorization: Bearer`.
    /// Leave empty to call the public endpoint.
    pub cofl_token: String,
    /// Item tags (prefix match) to ignore, e.g. ["ENCHANTED_BOOK", "PET_SKIN"].
    pub blacklist: Vec<String>,
}

impl Default for LocalFinderConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            min_profit: 1_000_000,
            min_profit_percent: 8.0,
            max_price: 100_000_000,
            min_daily_sales: 5,
            min_cofl_volume: 3.0,
            prefilter_ratio: 0.9,
            max_cofl_lookups: 25,
            cofl_token: String::new(),
            blacklist: vec!["ENCHANTED_BOOK".into(), "PET_SKIN".into(), "RUNE".into()],
        }
    }
}

/// Coins received after Hypixel's listing fee and the 1% claim fee over 1M.
pub fn after_tax(price: u64) -> u64 {
    let listing = if price >= 100_000_000 {
        0.025
    } else if price >= 10_000_000 {
        0.02
    } else {
        0.01
    };
    let claim = if price >= 1_000_000 { 0.01 } else { 0.0 };
    (price as f64 * (1.0 - listing - claim)) as u64
}

// ── Hypixel ──────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct AhPage {
    #[serde(rename = "totalPages")]
    total_pages: u32,
    #[serde(rename = "lastUpdated")]
    last_updated: i64,
    auctions: Vec<RawAuction>,
}

#[derive(Deserialize, Clone)]
struct RawAuction {
    uuid: String,
    item_name: String,
    item_bytes: String,
    starting_bid: u64,
    #[serde(default)]
    bin: bool,
    start: i64,
}

/// What we read out of an item's NBT.
struct ItemInfo {
    tag: String,
    /// SkyAH variant id for pets, e.g. `LEGENDARY_L100`.
    variant: Option<String>,
    count: u64,
}

fn parse_item(item_bytes: &str) -> Option<ItemInfo> {
    let gz = base64::engine::general_purpose::STANDARD
        .decode(item_bytes)
        .ok()?;
    let mut raw = Vec::new();
    flate2::read::GzDecoder::new(&gz[..])
        .read_to_end(&mut raw)
        .ok()?;
    let nbt = simdnbt::owned::read(&mut Cursor::new(&raw[..])).ok()?;
    if nbt.is_none() {
        return None;
    }
    let item = nbt.list("i")?.compounds()?.first()?.clone();
    let count = item.byte("Count").map(|c| c.max(1) as u64).unwrap_or(1);
    let ea = item.compound("tag")?.compound("ExtraAttributes")?;
    let id = ea.string("id")?.to_str().into_owned();

    if id == "PET" {
        let info: serde_json::Value =
            serde_json::from_str(&ea.string("petInfo")?.to_str()).ok()?;
        let pet_type = info.get("type")?.as_str()?;
        let tier = info.get("tier")?.as_str()?;
        return Some(ItemInfo {
            tag: format!("PET_{}", pet_type),
            variant: Some(tier.to_string()), // level band added from the name
            count,
        });
    }
    Some(ItemInfo {
        tag: id,
        variant: None,
        count,
    })
}

/// SkyAH pet variant ids: `<TIER>_L1`, `_L2_49`, `_L50_74`, `_L75_89`,
/// `_L90_99`, `_L100` (`_L200` style bands for golden dragon fall back to item).
fn pet_variant(tier: &str, item_name: &str) -> String {
    let lvl: u32 = item_name
        .split("Lvl ")
        .nth(1)
        .and_then(|s| s.split(']').next())
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(1);
    let band = match lvl {
        0..=1 => "L1",
        2..=49 => "L2_49",
        50..=74 => "L50_74",
        75..=89 => "L75_89",
        90..=99 => "L90_99",
        _ => "L100",
    };
    format!("{}_{}", tier, band)
}

// ── SkyAH ────────────────────────────────────────────────────────────────

#[derive(Deserialize, Default, Clone)]
struct Window {
    #[serde(default)]
    sales: u64,
    p50: Option<f64>,
}

#[derive(Deserialize, Clone)]
struct SkyAhEntry {
    lowest_bin: Option<f64>,
    #[serde(default)]
    day: Window,
    #[serde(default)]
    variants: HashMap<String, SkyAhEntry>,
}

#[derive(Deserialize)]
struct SkyAhFile {
    next_update: i64,
    items: HashMap<String, SkyAhEntry>,
}

/// Reference price for the pre-filter: the higher of the 24h median and the
/// current lowest BIN, so upgraded items are not filtered out too early.
fn skyah_reference(entry: &SkyAhEntry, min_sales: u64) -> Option<f64> {
    if entry.day.sales < min_sales {
        return None;
    }
    let median = entry.day.p50?;
    Some(entry.lowest_bin.map_or(median, |l| l.max(median)))
}

// ── Coflnet ──────────────────────────────────────────────────────────────

#[derive(Deserialize, Debug)]
struct PriceEstimate {
    lbin: i64,
    median: i64,
    volume: f64,
    #[serde(rename = "fastSell", default)]
    fast_sell: i64,
}

// ── Finder ───────────────────────────────────────────────────────────────

pub struct LocalFinder {
    cfg: LocalFinderConfig,
    http: reqwest::Client,
    skyah: HashMap<String, SkyAhEntry>,
    skyah_next: i64,
    skyah_etag: Option<String>,
    last_updated: i64,
    seen: HashSet<String>,
    seen_order: VecDeque<String>,
}

impl LocalFinder {
    pub fn new(cfg: LocalFinderConfig) -> Self {
        let http = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .timeout(Duration::from_secs(15))
            .build()
            .expect("reqwest client");
        Self {
            cfg,
            http,
            skyah: HashMap::new(),
            skyah_next: 0,
            skyah_etag: None,
            last_updated: 0,
            seen: HashSet::new(),
            seen_order: VecDeque::new(),
        }
    }

    /// Spawn the finder loop. Flips go to `tx` like any other flip source.
    pub fn spawn(cfg: LocalFinderConfig, tx: UnboundedSender<CoflEvent>) {
        if !cfg.enabled {
            return;
        }
        info!(
            "[LocalFinder] Enabled (min profit {}, min {}%, max price {}, cofl token: {})",
            cfg.min_profit,
            cfg.min_profit_percent,
            cfg.max_price,
            if cfg.cofl_token.is_empty() { "no" } else { "yes" }
        );
        tokio::spawn(async move {
            let mut finder = LocalFinder::new(cfg);
            loop {
                let delay = match finder.tick(&tx).await {
                    Ok(d) => d,
                    Err(e) => {
                        warn!("[LocalFinder] {}", e);
                        Duration::from_secs(5)
                    }
                };
                if tx.is_closed() {
                    return;
                }
                tokio::time::sleep(delay).await;
            }
        });
    }

    async fn refresh_skyah(&mut self) -> anyhow::Result<()> {
        let now = chrono::Utc::now().timestamp();
        if now < self.skyah_next {
            return Ok(());
        }
        let mut req = self.http.get(SKYAH_PRICES);
        if let Some(etag) = &self.skyah_etag {
            req = req.header("If-None-Match", etag);
        }
        let res = req.send().await?;
        if res.status() == reqwest::StatusCode::NOT_MODIFIED {
            self.skyah_next = now + 30;
            return Ok(());
        }
        let res = res.error_for_status()?;
        self.skyah_etag = res
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        let file: SkyAhFile = res.json().await?;
        // Ask again a few seconds after the next rebuild.
        self.skyah_next = file.next_update.max(now + 30) + 5;
        debug!("[LocalFinder] SkyAH prices loaded: {} items", file.items.len());
        self.skyah = file.items;
        Ok(())
    }

    async fn fetch_page(&self, page: u32) -> anyhow::Result<AhPage> {
        Ok(self
            .http
            .get(format!("{}?page={}", HYPIXEL_AH, page))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    /// One cycle. Returns how long to wait before the next one.
    async fn tick(&mut self, tx: &UnboundedSender<CoflEvent>) -> anyhow::Result<Duration> {
        if let Err(e) = self.refresh_skyah().await {
            warn!("[LocalFinder] SkyAH refresh failed: {}", e);
        }
        let first = self.fetch_page(0).await?;
        if first.last_updated == self.last_updated {
            return Ok(Duration::from_secs(2));
        }
        let since = if self.last_updated == 0 {
            first.last_updated - 120_000
        } else {
            self.last_updated
        };
        self.last_updated = first.last_updated;

        let pages = futures::future::join_all(
            (1..first.total_pages).map(|p| self.fetch_page(p)),
        )
        .await;
        let mut auctions = first.auctions;
        for p in pages.into_iter().flatten() {
            auctions.extend(p.auctions);
        }

        let started = Instant::now();
        let candidates = self.prefilter(auctions, since);
        let mut found = 0;
        for (auction, ref_price) in candidates.into_iter().take(self.cfg.max_cofl_lookups) {
            match self.evaluate(&auction, ref_price).await {
                Ok(Some(flip)) => {
                    found += 1;
                    info!(
                        "[LocalFinder] Flip: {} for {} (target {}, +{:.1}%)",
                        flip.item_name,
                        flip.starting_bid,
                        flip.target,
                        flip.profit_perc.unwrap_or(0.0)
                    );
                    if tx.send(CoflEvent::AuctionFlip(flip)).is_err() {
                        break;
                    }
                }
                Ok(None) => {}
                Err(e) => debug!("[LocalFinder] Cofl lookup for {} failed: {}", auction.uuid, e),
            }
        }
        debug!(
            "[LocalFinder] Cycle done in {:?}, {} flips",
            started.elapsed(),
            found
        );

        // Hypixel rebuilds the file ~every 60 s; wake up just after the next one.
        let next = self.last_updated + 61_000 - chrono::Utc::now().timestamp_millis();
        Ok(Duration::from_millis(next.clamp(1_000, 65_000) as u64))
    }

    /// New BINs priced clearly under the SkyAH reference, best deals first.
    fn prefilter(&mut self, auctions: Vec<RawAuction>, since: i64) -> Vec<(RawAuction, f64)> {
        let mut out = Vec::new();
        for a in auctions {
            if !a.bin || a.start <= since || a.starting_bid > self.cfg.max_price {
                continue;
            }
            if self.seen.contains(&a.uuid) {
                continue;
            }
            let Some(info) = parse_item(&a.item_bytes) else { continue };
            if self.cfg.blacklist.iter().any(|b| info.tag.starts_with(b.as_str())) {
                continue;
            }
            let Some(entry) = self.skyah.get(&info.tag) else { continue };
            let entry = match &info.variant {
                Some(tier) => entry
                    .variants
                    .get(&pet_variant(tier, &a.item_name))
                    .unwrap_or(entry),
                None => entry,
            };
            let Some(reference) = skyah_reference(entry, self.cfg.min_daily_sales) else {
                continue;
            };
            let unit_price = a.starting_bid as f64 / info.count as f64;
            if unit_price <= reference * self.cfg.prefilter_ratio {
                out.push((a, unit_price / reference));
            }
        }
        out.sort_by(|x, y| x.1.partial_cmp(&y.1).unwrap_or(std::cmp::Ordering::Equal));
        out
    }

    /// Price the exact item with Coflnet and decide whether it is a flip.
    async fn evaluate(&mut self, a: &RawAuction, _ratio: f64) -> anyhow::Result<Option<Flip>> {
        self.mark_seen(&a.uuid);
        // Hypixel's item_bytes is already the base64 gzipped inventory NBT
        // this endpoint expects.
        let body = serde_json::json!({
            "chestName": "Auction House",
            "fullInventoryNbt": a.item_bytes,
            "jsonNbt": null,
            "position": null,
            "senderContactId": null,
            "server": null,
            "heldSlot": null,
        });
        let mut req = self.http.post(COFL_PRICE_NBT).json(&body);
        if !self.cfg.cofl_token.is_empty() {
            req = req.bearer_auth(&self.cfg.cofl_token);
        }
        let estimates: Vec<Option<PriceEstimate>> =
            req.send().await?.error_for_status()?.json().await?;
        let Some(Some(est)) = estimates.into_iter().next() else {
            return Ok(None);
        };
        if est.volume < self.cfg.min_cofl_volume || est.median <= 0 {
            return Ok(None);
        }

        // Resell price: never above the cheapest matching BIN still listed
        // (we would have to undercut it) and never above the median. Coflnet's
        // lbin may be this very auction, so ignore lbins at/below our price.
        let mut target = est.median;
        if est.lbin > a.starting_bid as i64 {
            target = target.min(est.lbin);
        }
        if est.fast_sell > 0 {
            // Blend in the "sells within 5 minutes" price for safety.
            target = (target + est.fast_sell.min(target)) / 2;
        }
        let target = target.max(0) as u64;
        let profit = after_tax(target) as i64 - a.starting_bid as i64;
        let pct = profit as f64 * 100.0 / a.starting_bid.max(1) as f64;
        if profit < self.cfg.min_profit as i64 || pct < self.cfg.min_profit_percent {
            debug!(
                "[LocalFinder] Skip {} {} -> {} (+{}, {:.1}%)",
                a.item_name, a.starting_bid, target, profit, pct
            );
            return Ok(None);
        }

        Ok(Some(Flip {
            item_name: a.item_name.clone(),
            starting_bid: a.starting_bid,
            target,
            finder: Some(FINDER_NAME.to_string()),
            profit_perc: Some(pct),
            // BIN auctions can be bought 20 s after listing.
            purchase_at_ms: Some(a.start + 20_000),
            uuid: Some(a.uuid.replace('-', "")),
            list_at: Some(target),
        }))
    }

    fn mark_seen(&mut self, uuid: &str) {
        if self.seen.insert(uuid.to_string()) {
            self.seen_order.push_back(uuid.to_string());
            if self.seen_order.len() > 20_000 {
                if let Some(old) = self.seen_order.pop_front() {
                    self.seen.remove(&old);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tax_brackets() {
        assert_eq!(after_tax(500_000), 495_000);
        assert_eq!(after_tax(5_000_000), 4_900_000);
        assert_eq!(after_tax(50_000_000), 48_500_000);
        assert_eq!(after_tax(200_000_000), 193_000_000);
    }

    #[test]
    fn pet_bands() {
        assert_eq!(pet_variant("LEGENDARY", "[Lvl 100] Ender Dragon"), "LEGENDARY_L100");
        assert_eq!(pet_variant("EPIC", "[Lvl 1] Ender Dragon"), "EPIC_L1");
        assert_eq!(pet_variant("EPIC", "[Lvl 82] Blue Whale"), "EPIC_L75_89");
    }
}

#[cfg(test)]
mod live_tests {
    use super::*;

    /// Hits the real APIs: `cargo test --lib local_finder -- --ignored --nocapture`
    #[tokio::test]
    #[ignore]
    async fn live_smoke() {
        let mut f = LocalFinder::new(LocalFinderConfig { enabled: true, ..Default::default() });
        f.refresh_skyah().await.unwrap();
        let page = f.fetch_page(0).await.unwrap();
        let parsed = page.auctions.iter().filter(|a| parse_item(&a.item_bytes).is_some()).count();
        println!("skyah items {}, page0 auctions {}, parsed {}", f.skyah.len(), page.auctions.len(), parsed);
        assert!(parsed * 10 >= page.auctions.len() * 9);
        let a = page.auctions.iter().find(|a| a.bin).unwrap().clone();
        let r = f.evaluate(&a, 1.0).await;
        println!("evaluate {} {} -> {:?}", a.item_name, a.starting_bid, r.map(|o| o.map(|x| x.target)));
        let c = f.prefilter(page.auctions, 0);
        println!("candidates on page0: {}", c.len());
    }
}
