use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};

use grpc_client::TransactionFormat;
use heed::types::{Bytes, SerdeBincode};
use heed::{Database, Env};
use log::{info, warn};
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;
use tokio::time::Duration;
use transaction_monitor::tx_subscriber::TxSubscriber;
use utils::balance_change::balance_changes_of_grpc;
use utils::parse_rpc_fetched_json::BalanceChange;

const WSOL: &str = "So11111111111111111111111111111111111111112";
const USD1: &str = "USD1ttGY1N17NEEHLmELoaybftRBUSErhqYiQzvEmuB";
const USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const USDT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";

/// 从 `TransactionFormat` 还原完整账户列表（静态 + ALT 加载）。
///
/// 4.x 的 `TransactionFormat` 不再直接存 `account_keys`，需由消息静态键 +
/// `meta.loaded_addresses` 现算。
fn keys_of(tx: &TransactionFormat) -> Vec<Pubkey> {
    let (writable, readonly) = tx
        .meta
        .as_ref()
        .map(|m| (m.loaded_addresses.writable.as_slice(), m.loaded_addresses.readonly.as_slice()))
        .unwrap_or((&[], &[]));
    utils::tx_parse::account_keys_of(&tx.transaction.message, writable, readonly)
}

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
    #[serde(default)]
    pub base_position: i128,
    #[serde(default)]
    pub last_base_after_balance: u64,
    /// **`last_base_after_balance` 记在哪个账户上** —— ATA，还是
    /// `create_account_with_seed` 自建的那个 token account。
    ///
    /// 只有金额的话，卖出侧无法判断该 `close` 哪个账户、或把指令里写死的 ATA 改写到哪。
    /// 与金额来自**同一个 `BalanceChange`**（在 `add_success_trade` 里一起写入），永远同源。
    #[serde(default)]
    pub last_base_token_account: Pubkey,
}

impl TokenPnl {
    fn add_success_trade(
        &mut self,
        quote_change: i128,
        buy_cost_delta: i128,
        sol_gas: i128,
        base_change: i128,
        base_after_balance: u64,
        base_token_account: Pubkey,
        quote_mint: Pubkey,
        payer: Pubkey,
        slot: u64,
    ) {
        self.quote_pnl += quote_change;
        self.buy_cost_total += buy_cost_delta;
        self.sol_gas_cost += sol_gas;
        self.base_position += base_change;
        self.last_base_after_balance = base_after_balance;
        // 账户与金额同一个 `BalanceChange` 进来，保证两个字段不会错配。
        self.last_base_token_account = base_token_account;
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

    pub fn is_settled(&self) -> bool {
        self.success_tx_count >= 1
            && self.buy_cost_total > 0
            && self.last_base_after_balance == 0
            && self.base_position <= 0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettledTokenPnlDetail {
    pub payer: String,
    pub mint: String,
    pub quote_mint: String,
    pub quote_pnl: i128,
    pub buy_cost_total: i128,
    pub sol_gas_cost: i128,
    pub return_pct: Option<f64>,
    pub success_tx_count: usize,
    pub last_slot: u64,
}

/// 「某个账户上有多少余额」这一对：**金额 + 账户**。
///
/// 只有金额是不够的 —— 同一个 `(owner, mint)` 的仓位可能落在 ATA，
/// 也可能落在 `create_account_with_seed` 自建的 token account 里。
/// 卖出侧拿不到「是哪个账户」就无法决定该去 close 哪个账户、或往哪条指令上改写。
///
/// 查余额的入口 [`PnlTracker::query_balances`] 返回的是它的 `Vec`（**所有**有正余额的账户），
/// 由调用方自己挑。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BalanceSnapshot {
    pub amount: u64,
    /// 余额实际所在的 token account（ATA 或 `create_account_with_seed` 自建账户）。
    pub token_account: Pubkey,
}

pub struct PnlTracker {
    targets: RwLock<HashSet<Pubkey>>,
    cache: RwLock<HashMap<(Pubkey, Pubkey), TokenPnl>>,
    /// **原生 SOL 余额镜像**：`mint == Pubkey::default()` 的最近一次链上 `after_balance`。
    ///
    /// 自有库体系里「SOL」统一用 [`Pubkey::default`] 表示（与 `QUOTE_CURRENCIES` 一致），
    /// 不会与任何 SPL mint 冲突。给买入侧当 `pre_balance` 快照用 ——
    /// 省掉热路径里一次 `get_balance` RPC。
    sol_balances: RwLock<HashMap<Pubkey, u64>>,
    db: Option<PnlDb>,
    latest_slot: AtomicU64,
    /// 清仓结算回调：key=payer，仓位清零时 spawn 对应 payer 的所有回调
    settle_callbacks: RwLock<HashMap<Pubkey, Vec<Arc<dyn Fn(Pubkey, Pubkey, TokenPnl) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>>>>,
}

struct PnlDb {
    env: Env,
    db: Database<Bytes, SerdeBincode<TokenPnl>>,
}

impl PnlDb {
    fn open(path: &PathBuf) -> Result<Self, String> {
        std::fs::create_dir_all(path).map_err(|e| e.to_string())?;
        let env = unsafe {
            heed::EnvOpenOptions::new()
                .map_size(256 * 1024 * 1024)
                .max_dbs(1)
                .open(path)
                .map_err(|e| e.to_string())?
        };
        let mut wtxn = env.write_txn().map_err(|e| e.to_string())?;
        let db = env
            .create_database(&mut wtxn, Some("pnl_tracker"))
            .map_err(|e| e.to_string())?;
        wtxn.commit().map_err(|e| e.to_string())?;
        Ok(Self { env, db })
    }
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
        let db = match PnlDb::open(&db_path) {
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
        // 旧布局（`TokenPnl` 加 `last_base_token_account` 之前）的条目会因为 bincode
        // 非自描述而解码失败。**不做迁移，直接跳过** —— 那些 (payer, mint) 的统计
        // 从下一笔观察到的交易重新开始，不影响运行。
        let mut skipped_legacy = 0usize;
        if let Some(db_ref) = db.as_ref() {
            if let Ok(rtxn) = db_ref.env.read_txn()
                && let Ok(iter) = db_ref.db.iter(&rtxn)
            {
                for item in iter {
                    let Ok((key, pnl)) = item else {
                        // heed 的迭代器先移动游标再解码，所以这里 continue 之后
                        // 后面的条目照常读得到，不会卡在坏条目上。
                        skipped_legacy += 1;
                        continue;
                    };
                    let Ok(key_str) = std::str::from_utf8(key) else {
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
                    cache.insert((payer, mint), pnl);
                }
            }
            if skipped_legacy > 0 {
                warn!(
                    "[pnl-tracker] 跳过 {} 条旧布局条目（TokenPnl 新增 last_base_token_account，bincode 布局已变）。\
                     这些 (payer, mint) 的 PnL 统计会从下一笔交易重新累计。",
                    skipped_legacy
                );
            }
        }

        Self {
            targets: RwLock::new(HashSet::new()),
            cache: RwLock::new(cache),
            sol_balances: RwLock::new(HashMap::new()),
            db,
            latest_slot: AtomicU64::new(0),
            settle_callbacks: RwLock::new(HashMap::new()),
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
        self.latest_slot.fetch_max(tx.slot, Ordering::Relaxed);
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

    /// 查询 `payer` 的**原生 SOL 余额镜像**（`mint == Pubkey::default()`）。
    ///
    /// - `Some(bal)` = 最近一次在链上观察到的该 payer 原生 SOL `after_balance`（lamports）
    /// - `None`      = 启动后还没观察到该 payer 的任何余额变化（调用方自行回落 RPC）
    ///
    /// ⚠️ 这是**事件流镜像**，不是实时账面：它只反映「最后一笔被观察到的交易之后」的余额。
    /// 作为 `pre_balance` 快照用时方向是安全的 —— 镜像偏旧（偏高）只会让补足量偏小，
    /// 不会超买；偏新（偏低）才可能超买，那需要该 payer 在两次观察之间**收到** SOL。
    pub async fn query_sol_balance(&self, payer: &Pubkey) -> Option<u64> {
        self.sol_balances.read().await.get(payer).copied()
    }

    /// **只读内存镜像**（**不碰 RPC、不碰磁盘**，立即返回）。
    ///
    /// 返回最近一次为 `(payer, mint)` 观察到的 base 余额，以及它记在哪个账户上。
    /// 数据源是 `cache`（内存 `HashMap`）：启动时由 LMDB 预热，之后随 gRPC 事件流更新。
    ///
    /// ⚠️ `token_account` 可能是 [`Pubkey::default()`] —— 那是 `BalanceChange.token_account`
    /// **解析失败**（`account_index` 越界等）的标记，此时**只有 `amount` 可信**。
    /// 要把账户拿去构造 / 改写指令，请用 [`Self::query_balances`]，
    /// 它会把这种情况当成 miss、改走链上，绝不会把 `default()` 当账户交出去。
    ///
    /// `None` = 镜像里根本没有这个 `(payer, mint)`（没注册成 target，或还没观察到任何余额变化）。
    pub async fn query_balance_mirror(&self, payer: &Pubkey, mint: &Pubkey) -> Option<BalanceSnapshot> {
        let cache = self.cache.read().await;
        let pnl = cache.get(&(*payer, *mint))?;
        Some(BalanceSnapshot {
            amount: pnl.last_base_after_balance,
            token_account: pnl.last_base_token_account,
        })
    }

    /// **把 `(payer, mint)` 的余额尽量查全**：返回**所有**查到有正余额的账户。
    ///
    /// # 职责边界
    ///
    /// 本接口**不挑**账户、不做优先级、不替调用方决定该用哪个 —— 它只负责
    /// 「尽可能完整地查」，把「余额落在哪几个账户」如实交回，由调用方自己筛
    /// （比如卖出侧只认自己的 seed 账户，那它自己 `find`）。
    ///
    /// # 取值顺序
    ///
    /// 1. **内存镜像命中**（`cache` 里有 `(payer, mint)` 且账户已知）→ 直接返回镜像里那一对，
    ///    **不发 RPC**。这个 `cache` 启动时由 LMDB 预热、之后随事件流更新，读路径只碰内存。
    ///    ⚠️ 镜像上游按 `(owner, mint)` 折叠，**天然单值**，所以缓存最多只能给回 **1** 个账户；
    ///    「多个账户都有余额」这种情况只有走第 2 步才看得见。
    /// 2. 否则把 `accounts` **全部**查一遍（**不早退** —— 早退就收不全），返回所有 `amount > 0` 的。
    ///
    /// # 会丢的信息
    ///
    /// 返回 `Vec` 区分不了「查了、确实没余额」和「账户不存在 / RPC 查不到」—— 两者都是空 `Vec`。
    /// 需要这个区分的老调用方（如 `alt_sniper` 依赖的 `Some(0)` vs `None`）暂时仍用原接口。
    ///
    /// # 不写回缓存
    ///
    /// 链上结果**不**回填 `cache`：它是 gRPC 事件流的镜像，混入 RPC 快照会让
    /// 「镜像可能落后于链上」这个前提失效（见 [`Self::query_sol_balance`] 的说明）。
    ///
    /// # 原生 SOL
    ///
    /// 原生 SOL 没有 token account 的概念，不在本接口范围内 —— 用 [`Self::query_sol_balance`]。
    ///
    /// # 例子（暂未接线，供迁移时参考）
    ///
    /// ```ignore
    /// let found = tracker.query_balances(&payer, &mint, vec![ata, seed, extra], &rpc).await;
    /// // 调用方自己挑该用哪个：
    /// let on_my_account = found.iter().find(|s| s.token_account == my_account);
    /// ```
    pub async fn query_balances(
        &self,
        payer: &Pubkey,
        mint: &Pubkey,
        accounts: Vec<Pubkey>,
        rpc: &solana_client::nonblocking::rpc_client::RpcClient,
    ) -> Vec<BalanceSnapshot> {
        // ── 1. 内存镜像：有记录就以它为准 ──
        if let Some(snap) = self.query_balance_mirror(payer, mint).await
            // 账户解析失败时是 `default()` —— 不是有效账户，当作没有镜像，去查链上。
            && snap.token_account != Pubkey::default()
        {
            return if snap.amount > 0 { vec![snap] } else { Vec::new() };
        }

        // ── 2. 链上：把给到的账户**全部**查完，不早退 ──
        let mut found = Vec::new();
        for token_account in accounts {
            if let Some(amount) = utils::get_ata_balance(&token_account, rpc).await
                && amount > 0
            {
                found.push(BalanceSnapshot { amount, token_account });
            }
        }
        found
    }

    pub async fn query_settled_token_pnl_detail(
        &self,
        payer: &Pubkey,
        mint: &Pubkey,
    ) -> Option<SettledTokenPnlDetail> {
        let pnl = self.query_token_pnl(payer, mint).await?;
        if !pnl.is_settled() {
            return None;
        }
        let return_pct =
            (pnl.buy_cost_total > 0).then_some(pnl.quote_pnl as f64 / pnl.buy_cost_total as f64);
        Some(SettledTokenPnlDetail {
            payer: payer.to_string(),
            mint: mint.to_string(),
            quote_mint: pnl.quote_mint,
            quote_pnl: pnl.quote_pnl,
            buy_cost_total: pnl.buy_cost_total,
            sol_gas_cost: pnl.sol_gas_cost,
            return_pct,
            success_tx_count: pnl.success_tx_count,
            last_slot: pnl.last_slot,
        })
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

    /// 注册清仓结算回调（async）。仅当 `payer` 的仓位清零时 spawn 回调。
    pub async fn on_settle<F, Fut>(&self, payer: Pubkey, f: F)
    where
        F: Fn(Pubkey, Pubkey, TokenPnl) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        self.settle_callbacks
            .write()
            .await
            .entry(payer)
            .or_default()
            .push(Arc::new(move |p, m, t| Box::pin(f(p, m, t))));
    }

    /// GC：删除超过 `ttl_slots` 未更新的条目（内存 + LMDB）。
    /// 默认 TTL = 216_000 slots（~1 天）。
    pub async fn gc(&self, ttl_slots: u64) -> usize {
        let current = self.latest_slot.load(Ordering::Relaxed);
        if current < ttl_slots {
            return 0; // 刚启动，slot 还不够
        }
        let cutoff = current.saturating_sub(ttl_slots);

        // 1. 内存清理
        let stale_keys: Vec<(Pubkey, Pubkey)> = {
            self.cache
                .read()
                .await
                .iter()
                .filter(|(_, pnl)| pnl.last_slot < cutoff)
                .map(|(k, _)| *k)
                .collect()
        };

        let mut cache = self.cache.write().await;
        for key in &stale_keys {
            cache.remove(key);
        }
        drop(cache);

        // 2. LMDB 清理
        let mut db_removed = 0usize;
        if let Some(db) = self.db.as_ref() {
            let stale_db_keys: Vec<String> = {
                let mut keys = Vec::new();
                if let Ok(rtxn) = db.env.read_txn()
                    && let Ok(iter) = db.db.iter(&rtxn)
                {
                    for item in iter {
                        let Ok((key_bytes, pnl)) = item else { continue };
                        if pnl.last_slot < cutoff {
                            if let Ok(key_str) = std::str::from_utf8(key_bytes) {
                                keys.push(key_str.to_string());
                            }
                        }
                    }
                }
                keys
            };

            if !stale_db_keys.is_empty() {
                match db.env.write_txn() {
                    Ok(mut wtxn) => {
                        for key_str in &stale_db_keys {
                            let _ = db.db.delete(&mut wtxn, key_str.as_bytes());
                            db_removed += 1;
                        }
                        if let Err(e) = wtxn.commit() {
                            warn!("[pnl-tracker] GC LMDB commit failed: {}", e);
                        }
                    }
                    Err(e) => {
                        warn!("[pnl-tracker] GC LMDB write_txn failed: {}", e);
                    }
                }
            }
        }

        let total = stale_keys.len() + db_removed;
        if total > 0 {
            info!(
                "[pnl-tracker] GC removed {} entries (mem={}, db={}, cutoff_slot={})",
                total,
                stale_keys.len(),
                db_removed,
                cutoff
            );
        }
        total
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

        // **原生 SOL 余额镜像**：只要本笔有 `mint == Pubkey::default()` 的余额变化就刷新，
        // 与有没有 base 代币变化无关（wrap / tip / 纯转账 / 租金都要记）。
        // 放在 `base_change` 早退之前 —— 否则 SOL-only 的交易会被漏掉。
        if let Some(sol) = self_changes.iter().find(|c| c.mint == Pubkey::default()) {
            self.sol_balances.write().await.insert(target, sol.after_balance);
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
            base_change.change,
            base_change.after_balance,
            base_change.token_account,
            quote_mint,
            target,
            slot,
        );

        let settled = stat.is_settled();
        let settled_payload = if settled { Some(stat.clone()) } else { None };

        // LMDB 持久化
        if let Some(db) = self.db.as_ref() {
            let key = format!("{}:{}", target, base_change.mint);
            match db.env.write_txn() {
                Ok(mut wtxn) => {
                    if let Err(e) = db.db.put(&mut wtxn, key.as_bytes(), stat) {
                        warn!("[pnl-tracker] LMDB put failed key={} err={}", key, e);
                    }
                    if let Err(e) = wtxn.commit() {
                        warn!("[pnl-tracker] LMDB commit failed key={} err={}", key, e);
                    }
                }
                Err(e) => {
                    warn!("[pnl-tracker] LMDB write_txn failed key={} err={}", key, e);
                }
            }
        }

        // 释放写锁后再触发回调，避免死锁
        drop(cache);

        if let Some(payload) = settled_payload {
            let cbs = self.settle_callbacks.read().await;
            if let Some(payer_cbs) = cbs.get(&target) {
                let payer = target;
                let mint = base_change.mint;
                for cb in payer_cbs.iter() {
                    let cb = cb.clone();
                    let p = payload.clone();
                    tokio::spawn(async move { cb(payer, mint, p).await });
                }
            }
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
                    // combine 失败时 fallback：取变化量较大的一方
                    let pick = if sol.change.abs() >= wsol.change.abs() { sol } else { wsol };
                    return (Pubkey::default(), pick.clone());
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

/// 查询 `payer` 的原生 SOL 余额镜像（全局 tracker，`mint == Pubkey::default()`）。
///
/// `None` = 启动后还没观察过该 payer（调用方自行回落 RPC）。
pub async fn sol_balance(payer: &Pubkey) -> Option<u64> {
    global_tracker().query_sol_balance(payer).await
}

/// 全局 tracker 的 [`PnlTracker::query_balance_mirror`]。
///
/// **不碰 RPC**：镜像没有就返回 `None`，适合放在「先看内存、miss 再决定要不要花钱查链」
/// 的热路径上。
pub async fn balance_mirror(payer: &Pubkey, mint: &Pubkey) -> Option<BalanceSnapshot> {
    global_tracker().query_balance_mirror(payer, mint).await
}

/// 全局 tracker 的 [`PnlTracker::query_balances`]。
///
/// 缓存优先；miss 时把 `accounts` 全查一遍，返回**所有**有正余额的账户。
/// 账户清单（ATA / seed 派生账户 / 别的）由调用方给全 —— 这个 crate 不关心账户怎么派生，
/// 也不替调用方挑该用哪个。
pub async fn query_balances(
    payer: &Pubkey,
    mint: &Pubkey,
    accounts: Vec<Pubkey>,
    rpc: &solana_client::nonblocking::rpc_client::RpcClient,
) -> Vec<BalanceSnapshot> {
    global_tracker().query_balances(payer, mint, accounts, rpc).await
}

pub async fn query_settled_token_pnl_detail(
    payer: &Pubkey,
    mint: &Pubkey,
) -> Option<SettledTokenPnlDetail> {
    global_tracker()
        .query_settled_token_pnl_detail(payer, mint)
        .await
}

/// 注册清仓结算回调（全局，指定 payer，async）。
pub async fn on_settle<F, Fut>(payer: Pubkey, f: F)
where
    F: Fn(Pubkey, Pubkey, TokenPnl) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    global_tracker().on_settle(payer, f).await;
}

/// 启动后台 GC 任务：每 10 分钟清理超过 TTL（默认 ~1 天 = 216000 slots）的条目。
pub fn spawn_gc_task(ttl_slots: Option<u64>) {
    let ttl = ttl_slots.unwrap_or(216_000); // 默认 ~1 天
    let tracker = global_tracker();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(600)); // 每 10 分钟
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await; // 跳过第一次 tick（启动时 slot 可能为 0）
        loop {
            interval.tick().await;
            let removed = tracker.gc(ttl).await;
            if removed > 0 {
                info!("[pnl-tracker] GC cycle removed {} entries", removed);
            }
        }
    });
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
        let keys = keys_of(tx);
        Some(keys.iter().any(|k| targets.contains(k)))
    }

    async fn on_tx(self: Arc<Self>, tx: Arc<TransactionFormat>) {
        if let Err(e) = self.tracker.process_tx(&tx).await {
            let sig = tx.transaction.signatures.first().copied().unwrap_or_default();
            warn!("[PnlTrackerSubscriber] process tx failed sig={} err={}", sig, e);
        }
    }
}
