use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};

use grpc_client::TransactionFormat;
use log::warn;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use tokio::sync::RwLock;
use transaction_monitor::tx_subscriber::TxSubscriber;
use utils::balance_change::balance_changes_of_grpc;
use utils::parse_rpc_fetched_json::BalanceChange;

const WSOL: &str = "So11111111111111111111111111111111111111112";
const USD1: &str = "USD1ttGY1N17NEEHLmELoaybftRBUSErhqYiQzvEmuB";
const USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const USDT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";

static WSOL_MINT: LazyLock<Pubkey> = LazyLock::new(|| WSOL.parse().expect("invalid WSOL mint"));
static QUOTE_CURRENCIES: LazyLock<Vec<Pubkey>> = LazyLock::new(|| {
    vec![
        USD1.parse().expect("invalid USD1 mint"),
        USDC.parse().expect("invalid USDC mint"),
        USDT.parse().expect("invalid USDT mint"),
        *WSOL_MINT,
        Pubkey::default(),
    ]
});

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TokenPnl {
    pub payer: String,
    pub quote_pnl: i128,
    pub buy_cost_total: i128,
    pub sol_gas_cost: i128,
    pub quote_mint: String,
    pub success_tx_count: usize,
    pub last_slot: u64,
}

impl TokenPnl {
    fn add_success_trade(
        &mut self,
        quote_change: i128,
        buy_cost_delta: i128,
        sol_gas: i128,
        quote_mint: Pubkey,
        payer: Pubkey,
        slot: u64,
    ) {
        self.quote_pnl += quote_change;
        self.buy_cost_total += buy_cost_delta;
        self.sol_gas_cost += sol_gas;
        self.success_tx_count += 1;
        self.last_slot = slot;

        if self.quote_mint.is_empty() {
            self.quote_mint = quote_mint.to_string();
        }
        if self.payer.is_empty() {
            self.payer = payer.to_string();
        }
    }

    pub fn get_quote_mint(&self) -> Option<Pubkey> {
        self.quote_mint.parse().ok()
    }

    pub fn is_sol_based(&self) -> bool {
        self.quote_mint == Pubkey::default().to_string() || self.quote_mint == WSOL
    }
}

pub struct PnlTracker {
    targets: RwLock<HashSet<Pubkey>>,
    cache: RwLock<HashMap<(Pubkey, Pubkey), TokenPnl>>,
    db: Option<sled::Db>,
}

impl Default for PnlTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl PnlTracker {
    pub fn new() -> Self {
        Self::with_db_path(None)
    }

    pub fn with_db_path(path: Option<PathBuf>) -> Self {
        let db_path = path.unwrap_or_else(|| PathBuf::from("./data/pnl"));
        let db = match sled::open(&db_path) {
            Ok(db) => Some(db),
            Err(e) => {
                warn!(
                    "[pnl-tracker] open db failed path={} err={} (memory-only mode)",
                    db_path.display(),
                    e
                );
                None
            }
        };

        let mut cache = HashMap::new();
        if let Some(db_ref) = db.as_ref() {
            for item in db_ref.iter() {
                let Ok((key, value)) = item else { continue };
                let Ok(key_str) = std::str::from_utf8(&key) else {
                    continue;
                };
                let Some((payer_str, mint_str)) = key_str.split_once(':') else {
                    continue;
                };
                let (Ok(payer), Ok(mint)) =
                    (payer_str.parse::<Pubkey>(), mint_str.parse::<Pubkey>())
                else {
                    continue;
                };
                let Ok(pnl) = bincode::deserialize::<TokenPnl>(&value) else {
                    continue;
                };
                cache.insert((payer, mint), pnl);
            }
        }

        Self {
            targets: RwLock::new(HashSet::new()),
            cache: RwLock::new(cache),
            db,
        }
    }

    pub async fn register_target(&self, target: Pubkey) -> bool {
        self.targets.write().await.insert(target)
    }

    pub async fn unregister_target(&self, target: &Pubkey) -> bool {
        self.targets.write().await.remove(target)
    }

    pub async fn replace_targets(&self, targets: Vec<Pubkey>) {
        let mut g = self.targets.write().await;
        g.clear();
        g.extend(targets);
    }

    pub async fn clear_targets(&self) {
        self.targets.write().await.clear();
    }

    pub async fn list_targets(&self) -> Vec<Pubkey> {
        self.targets.read().await.iter().copied().collect()
    }

    pub async fn has_target(&self, target: &Pubkey) -> bool {
        self.targets.read().await.contains(target)
    }

    pub async fn process_tx(&self, tx: &TransactionFormat) -> Result<(), String> {
        let changes = balance_changes_of_grpc(tx).map_err(|e| e.to_string())?;
        if changes.is_empty() {
            return Ok(());
        }

        let targets = self.targets.read().await.clone();
        if targets.is_empty() {
            return Ok(());
        }

        for target in targets {
            self.apply_changes_for_target(tx.slot, target, &changes)
                .await;
        }

        Ok(())
    }

    pub async fn query_token_pnl(&self, payer: &Pubkey, mint: &Pubkey) -> Option<TokenPnl> {
        self.cache.read().await.get(&(*payer, *mint)).cloned()
    }

    pub async fn query_payer_pnl(&self, payer: &Pubkey) -> HashMap<Pubkey, TokenPnl> {
        self.cache
            .read()
            .await
            .iter()
            .filter_map(|((p, mint), pnl)| {
                if p == payer {
                    Some((*mint, pnl.clone()))
                } else {
                    None
                }
            })
            .collect()
    }

    pub async fn query_all_pnl(&self) -> HashMap<(Pubkey, Pubkey), TokenPnl> {
        self.cache.read().await.clone()
    }

    async fn apply_changes_for_target(&self, slot: u64, target: Pubkey, changes: &[BalanceChange]) {
        let self_changes: Vec<BalanceChange> = changes
            .iter()
            .filter(|c| c.owner == target)
            .cloned()
            .collect();

        if self_changes.is_empty() {
            return;
        }

        let (quote_mint, quote_change) = select_quote_change(&self_changes);
        let Some(base_change) = self_changes
            .iter()
            .find(|c| !QUOTE_CURRENCIES.contains(&c.mint))
            .cloned()
        else {
            return;
        };

        let is_sol_based = quote_mint == Pubkey::default() || quote_mint == *WSOL_MINT;
        let sol_gas = if !is_sol_based {
            self_changes
                .iter()
                .find(|c| c.mint == Pubkey::default())
                .map(|c| c.change)
                .unwrap_or(0)
        } else {
            0
        };

        let buy_cost_delta = if base_change.change > 0 && quote_change.change < 0 {
            -quote_change.change
        } else {
            0
        };

        let mut cache = self.cache.write().await;
        let stat = cache
            .entry((target, base_change.mint))
            .or_insert_with(TokenPnl::default);
        stat.add_success_trade(
            quote_change.change,
            buy_cost_delta,
            sol_gas,
            quote_mint,
            target,
            slot,
        );

        if let Some(db) = self.db.as_ref()
            && let Ok(value) = bincode::serialize(stat)
        {
            let key = format!("{}:{}", target, base_change.mint);
            let _ = db.insert(key.as_bytes(), value);
            let _ = db.flush();
        }
    }
}

fn select_quote_change(changes: &[BalanceChange]) -> (Pubkey, BalanceChange) {
    for &currency in QUOTE_CURRENCIES.iter() {
        if currency == Pubkey::default() || currency == *WSOL_MINT {
            let sol_change = changes.iter().find(|c| c.mint == Pubkey::default());
            let wsol_change = changes.iter().find(|c| c.mint == *WSOL_MINT);
            match (sol_change, wsol_change) {
                (Some(sol), Some(wsol)) => {
                    if let Some(combined) = sol.combine(wsol) {
                        return (Pubkey::default(), combined);
                    }
                }
                (Some(sol), None) => return (Pubkey::default(), sol.clone()),
                (None, Some(wsol)) => return (Pubkey::default(), wsol.clone()),
                (None, None) => {}
            }
        } else if let Some(change) = changes.iter().find(|c| c.mint == currency) {
            return (currency, change.clone());
        }
    }
    (Pubkey::default(), BalanceChange::default())
}

pub struct PnlTrackerSubscriber {
    tracker: Arc<PnlTracker>,
}

impl PnlTrackerSubscriber {
    pub fn new(tracker: Arc<PnlTracker>) -> Self {
        Self { tracker }
    }

    pub fn tracker(&self) -> Arc<PnlTracker> {
        self.tracker.clone()
    }
}

static GLOBAL_TRACKER: LazyLock<Arc<PnlTracker>> = LazyLock::new(|| Arc::new(PnlTracker::new()));

pub fn global_tracker() -> Arc<PnlTracker> {
    GLOBAL_TRACKER.clone()
}

pub fn global_subscriber() -> Arc<PnlTrackerSubscriber> {
    Arc::new(PnlTrackerSubscriber::new(global_tracker()))
}

pub async fn register_target(target: Pubkey) -> bool {
    global_tracker().register_target(target).await
}

pub async fn unregister_target(target: &Pubkey) -> bool {
    global_tracker().unregister_target(target).await
}

pub async fn replace_targets(targets: Vec<Pubkey>) {
    global_tracker().replace_targets(targets).await;
}

pub async fn list_targets() -> Vec<Pubkey> {
    global_tracker().list_targets().await
}

pub async fn query_token_pnl(payer: &Pubkey, mint: &Pubkey) -> Option<TokenPnl> {
    global_tracker().query_token_pnl(payer, mint).await
}

#[async_trait::async_trait]
impl TxSubscriber for PnlTrackerSubscriber {
    fn name(&self) -> &'static str {
        "PnlTrackerSubscriber"
    }

    async fn interested(&self, tx: &TransactionFormat) -> Option<bool> {
        let targets = self.tracker.targets.read().await;
        if targets.is_empty() {
            return Some(false);
        }
        Some(tx.account_keys.iter().any(|k| targets.contains(k)))
    }

    async fn on_tx(self: Arc<Self>, tx: Arc<TransactionFormat>) {
        if let Err(e) = self.tracker.process_tx(&tx).await {
            warn!(
                "[PnlTrackerSubscriber] process tx failed sig={} err={}",
                tx.signature, e
            );
        }
    }
}
