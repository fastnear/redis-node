//! Streams blocks out of an embedded `neard` node into one or two Redis streams.
//!
//! A single process can serve several finalities at once, because `finality` only picks the head
//! a sweep runs up to - every block below it is fetched by height from the canonical index and
//! built by the same code. Set `OPTIMISTIC_BLOCKS_KEY` to get the chain head alongside the final
//! stream instead of running a second node for it.
//!
//! Environment:
//!   REDIS_URL                     single URL (no commas - only `caching-saver` splits them)
//!   BLOCKS_KEY                    stream key for the primary output
//!   FINALITY                      JSON string: "final" | "near-final" | "optimistic"
//!   STREAM_TO_REDIS               "true" to actually XADD
//!   MAX_NUM_BLOCKS                XADD MAXLEN ~ N
//!   START_BLOCK                   where to start when the stream is empty
//!   RECEIPT_BACKFILL_DEPTH        blocks replayed to warm the receipt -> tx map (default 250)
//!   MISSING_RECEIPTS_WHITELIST    receipt ids allowed to have no tx hash
//!   LOG_BLOCKS                    write res/blocks_log.csv (default: STREAM_TO_REDIS)
//!   SWEEP_INTERVAL_MS             poll interval (default 50)
//!   NEARDATA_CHAIN_ID             "mainnet" | "testnet", falls back to CHAIN_ID
//!
//!   OPTIMISTIC_BLOCKS_KEY         set to also stream the chain head to this key
//!   OPTIMISTIC_MAX_NUM_BLOCKS     MAXLEN for that stream (default: MAX_NUM_BLOCKS)
//!   OPTIMISTIC_ENABLE_THRESHOLD   how close the primary output must be to its head before the
//!                                 optimistic one starts (default 200)
//!   BLOCK_CACHE_WINDOW            blocks kept for the second output to reuse (default 64,
//!                                 0 disables the cache)
//!   BLOCK_CACHE_MAX_BYTES         cache backstop (default 256 MiB)

use std::io::Write;
mod common;
mod redis_db;

use dotenv::dotenv;
use fastnear_neardata_fetcher::fetcher;
use fastnear_primitives::block_with_tx_hash::BlockWithTxHashes;
use fastnear_primitives::near_primitives::hash::hash;
use fastnear_primitives::near_primitives::hash::CryptoHash;
use fastnear_primitives::near_primitives::views::ReceiptView;
use near_indexer::near_primitives::hash::CryptoHash as BlockHash;
use near_indexer::near_primitives::types::{BlockHeight, Finality};
use redis_db::RedisDB;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::fs::{create_dir_all, File, OpenOptions};
use std::sync::Arc;
use std::time::Duration;

pub type BlockHashes = Vec<CryptoHash>;

const PROJECT_ID: &str = "redisnode";
const BLOCK_KEY: &str = "block";

const MAX_RETRIES: usize = 10;
const INITIAL_RETRY_DELAY: u64 = 100;
const RECEIPT_BACKFILL_DEPTH: u64 = 250;
const EMPTY_REDIS_DEPTH: u64 = 1000;
const OPTIMISTIC_DEPTH: u64 = 10;

/// The number of blocks of receipts to keep in the cache before we start cleaning up.
/// It's necessary to keep receipts in memory for longer than one block in order to support
/// reorgs when streaming optimistic blocks.
const RECEIPT_HASH_CLEANUP_BLOCKS: u64 = 10;

/// How many times to retry fetching or building a single block before giving up.
///
/// `near_indexer::streamer::start` logged and *skipped* a block on error, which then tripped the
/// skipped-block assert in `emit_block` on the next block and crash-looped the process. Now that
/// we drive the sweep ourselves we retry instead, which is strictly better.
const MAX_BUILD_RETRIES: usize = 10;

const DEFAULT_SWEEP_INTERVAL_MS: u64 = 50;

/// How far the primary output may lag its head before the optimistic output is allowed to start.
const DEFAULT_OPTIMISTIC_ENABLE_THRESHOLD: u64 = 200;

/// How many blocks ahead of the slowest cursor a built block is still worth caching.
const DEFAULT_BLOCK_CACHE_WINDOW: u64 = 64;

/// Backstop on the block cache. Typical mainnet blocks are 100-500 KB of JSON, so the window is
/// normally the binding constraint and this only matters on a run of unusually large blocks.
const DEFAULT_BLOCK_CACHE_MAX_BYTES: usize = 256 * 1024 * 1024;

/// How far behind its head an output has to be before it counts as backfilling rather than live.
const BACKFILL_GAP: u64 = 32;

/// How often (in sweep ticks) to log cursor/cache stats.
const STATS_EVERY_N_TICKS: u64 = 200;

#[derive(Default)]
pub struct TxCache {
    pub receipt_to_tx: HashMap<CryptoHash, CryptoHash>,
    pub block_hashes: HashMap<BlockHeight, BlockHashes>,
}

impl TxCache {
    pub fn peek_receipt_to_tx(&self, receipt_id: &CryptoHash) -> Option<CryptoHash> {
        self.receipt_to_tx.get(receipt_id).cloned()
    }

    pub fn store_receipt_to_tx(&mut self, receipt_id: &CryptoHash, tx_hash: &CryptoHash) {
        let old_tx_hash = self
            .receipt_to_tx
            .insert(receipt_id.clone(), tx_hash.clone());

        if let Some(old_tx_hash) = old_tx_hash {
            assert_eq!(
                &old_tx_hash, tx_hash,
                "Duplicate receipt_id: {} with different TX HASHES!",
                receipt_id
            );
            tracing::log::warn!(target: PROJECT_ID, "Duplicate receipt_id: {} old_tx_hash: {} new_tx_hash: {}", receipt_id, old_tx_hash, tx_hash);
        }
    }

    pub fn set_receipt_hashes_to_remove(
        &mut self,
        block_height: BlockHeight,
        receipt_hashes: BlockHashes,
    ) {
        self.block_hashes.insert(block_height, receipt_hashes);
    }

    pub fn clean_receipt_hashes_to_remove(&mut self, block_height: BlockHeight) {
        let receipt_hashes = self.block_hashes.remove(&block_height);
        if let Some(receipt_hashes) = receipt_hashes {
            for receipt_hash in receipt_hashes {
                self.receipt_to_tx.remove(&receipt_hash);
            }
        }
    }
}

async fn last_neardata_block_height() -> BlockHeight {
    let client = reqwest::Client::new();
    // `CHAIN_ID` is overloaded: `caching-saver` uses it as a raw Redis key prefix and may legally
    // be set to something like `mainnet_opt`, which `ChainId::try_from` rejects. `NEARDATA_CHAIN_ID`
    // lets the two binaries share an env file.
    let raw_chain_id = env::var("NEARDATA_CHAIN_ID")
        .or_else(|_| env::var("CHAIN_ID"))
        .expect("Neither NEARDATA_CHAIN_ID nor CHAIN_ID is set");
    let chain_id = fastnear_primitives::types::ChainId::try_from(raw_chain_id.clone())
        .unwrap_or_else(|_| {
            panic!(
                "Invalid neardata chain id {:?}. Set NEARDATA_CHAIN_ID to `mainnet` or `testnet`.",
                raw_chain_id
            )
        });
    let last_block_height = fetcher::fetch_last_block(&client, chain_id)
        .await
        .expect("Last block doesn't exists")
        .block
        .header
        .height;
    last_block_height
}

/// Cache of built `StreamerMessage`s, serialized once and keyed by **block hash**.
///
/// Keying by hash rather than height is load-bearing. A reorg rewrites the canonical height index,
/// so the same height can resolve to a different block over time; a hash miss is exactly the signal
/// that the block at that height changed and has to be rebuilt.
#[derive(Default)]
pub struct BlockCache {
    by_hash: HashMap<BlockHash, Arc<Vec<u8>>>,
    /// One-to-many on purpose: after a reorg two hashes share a height, and a
    /// `HashMap<BlockHeight, BlockHash>` would silently drop the orphan and leak it in `by_hash`.
    heights: BTreeMap<BlockHeight, Vec<BlockHash>>,
    bytes: usize,
    /// Admission window, in blocks ahead of the slowest output's cursor. Admission is the right
    /// knob here rather than eviction: any normal eviction policy drops the *lowest* heights,
    /// which is precisely what the lagging output needs next. `0` disables caching entirely.
    window: u64,
    max_bytes: usize,
}

impl BlockCache {
    pub fn new(window: u64, max_bytes: usize) -> Self {
        Self {
            window,
            max_bytes,
            ..Default::default()
        }
    }

    pub fn get(&self, block_hash: &BlockHash) -> Option<Arc<Vec<u8>>> {
        self.by_hash.get(block_hash).cloned()
    }

    pub fn admit(
        &mut self,
        block_height: BlockHeight,
        block_hash: BlockHash,
        bytes: &Arc<Vec<u8>>,
        keep_until: BlockHeight,
    ) {
        if self.window == 0
            || block_height > keep_until.saturating_add(self.window)
            || self.bytes + bytes.len() > self.max_bytes
            || self.by_hash.contains_key(&block_hash)
        {
            return;
        }
        self.bytes += bytes.len();
        self.by_hash.insert(block_hash, Arc::clone(bytes));
        self.heights
            .entry(block_height)
            .or_default()
            .push(block_hash);
    }

    pub fn prune_below(&mut self, block_height: BlockHeight) {
        if self.by_hash.is_empty() {
            return;
        }
        let keep = self.heights.split_off(&block_height);
        let dropped = std::mem::replace(&mut self.heights, keep);
        for (_, block_hashes) in dropped {
            for block_hash in block_hashes {
                if let Some(bytes) = self.by_hash.remove(&block_hash) {
                    self.bytes = self.bytes.saturating_sub(bytes.len());
                }
            }
        }
    }

    /// The cache only earns its keep while two outputs are actually sweeping. Setting the window
    /// to 0 (single output, or the optimistic output still held off) makes it provably inert.
    pub fn set_window(&mut self, window: u64) {
        if window == self.window {
            return;
        }
        self.window = window;
        if window == 0 {
            self.by_hash.clear();
            self.heights.clear();
            self.bytes = 0;
        }
    }

    pub fn len(&self) -> usize {
        self.by_hash.len()
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

/// Builds (or serves from cache) one block at a time, shared by every output.
///
/// It owns the whole `near_indexer::Indexer` rather than destructuring it: the third argument of
/// `build_streamer_message` is a `near_epoch_manager::shard_tracker::ShardTracker`, and
/// `near-epoch-manager` is not a dependency of this crate, so that type cannot be named here.
/// Passing `&self.indexer.shard_tracker` inline sidesteps it (same trick as `backfill_and_save.rs`).
///
/// NOTE: we deliberately never call `indexer.streamer()`. Driving the sweep here is what lets a
/// single process serve several finalities: `streamer::start` opens `<home>/data/indexer` with
/// `DB::open_default` (a second open panics) and relies on nearcore's process-global
/// `DELAYED_LOCAL_RECEIPTS_CACHE`, whose destructive `remove()` breaks if two builders race.
/// With one shared builder each (height, hash) is built at most once, exactly as before.
pub struct Builder {
    indexer: near_indexer::Indexer,
    cache: BlockCache,
}

impl Builder {
    pub fn new(indexer: near_indexer::Indexer, cache: BlockCache) -> Self {
        Self { indexer, cache }
    }

    /// Current head height at the given finality. `Finality::None` is the (reorg-able) chain head,
    /// `Finality::Final` the final head. This is the *only* thing finality controls.
    pub async fn head(&self, finality: &Finality) -> Option<BlockHeight> {
        match self
            .indexer
            .view_client
            .fetch_latest_block(finality.clone())
            .await
        {
            Ok(block) => Some(block.header.height),
            Err(err) => {
                tracing::log::warn!(target: PROJECT_ID, "Failed to fetch latest block at {:?}: {:?}", finality, err);
                None
            }
        }
    }

    /// Returns the serialized `StreamerMessage` for the canonical block at `block_height`, building
    /// it if it isn't cached. `None` means there is no block at that height (a skipped height).
    ///
    /// The `fetch_block_by_height` round trip is paid by every output on every block and is what
    /// makes reorgs safe: it re-resolves the canonical hash before the cache is consulted.
    pub async fn get_or_build(
        &mut self,
        block_height: BlockHeight,
        keep_until: BlockHeight,
    ) -> Option<(BlockHash, Arc<Vec<u8>>)> {
        let mut delay = Duration::from_millis(INITIAL_RETRY_DELAY);
        for attempt in 0..MAX_BUILD_RETRIES {
            let block = match self
                .indexer
                .view_client
                .fetch_block_by_height(block_height)
                .await
            {
                Ok(Some(block)) => block,
                Ok(None) => {
                    tracing::log::debug!(target: PROJECT_ID, "Skipping height {} - no block", block_height);
                    return None;
                }
                Err(err) => {
                    tracing::log::warn!(target: PROJECT_ID, "Attempt #{} failed to fetch block {}: {:?}", attempt, block_height, err);
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                    continue;
                }
            };

            let block_hash = block.header.hash;
            if let Some(bytes) = self.cache.get(&block_hash) {
                return Some((block_hash, bytes));
            }

            // Boxed because the future is large - `near_indexer::streamer::start` boxes it too.
            let streamer_message = match Box::pin(near_indexer::build_streamer_message(
                &self.indexer.view_client,
                block,
                &self.indexer.shard_tracker,
            ))
            .await
            {
                Ok(streamer_message) => streamer_message,
                Err(err) => {
                    tracing::log::warn!(target: PROJECT_ID, "Attempt #{} failed to build block {}: {:?}", attempt, block_height, err);
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                    continue;
                }
            };

            let bytes = Arc::new(
                serde_json::to_vec(&streamer_message).expect("Failed to serialize streamer message"),
            );
            self.cache.admit(block_height, block_hash, &bytes, keep_until);
            return Some((block_hash, bytes));
        }
        panic!(
            "Failed to fetch or build block {} after {} attempts",
            block_height, MAX_BUILD_RETRIES
        );
    }
}

/// Immutable configuration of one output stream.
pub struct OutputConfig {
    /// Short label used in logs to tell the outputs apart.
    pub name: &'static str,
    pub blocks_key: String,
    /// Only ever used to pick this output's head height.
    pub finality: Finality,
    pub max_num_blocks: Option<usize>,
    pub stream_to_redis: bool,
    pub log_blocks: bool,
    /// Whether a prev-hash mismatch is fatal. Optimistic streams see reorgs by design.
    pub strict_hash_check: bool,
    /// Whether exhausting the Redis write retries kills the process. Final streams must not have
    /// gaps (`caching-saver` assumes contiguity); the optimistic stream tolerates them.
    pub fatal_on_redis_failure: bool,
    pub receipt_backfill_depth: u64,
    pub missing_receipts_whitelist: Arc<HashSet<CryptoHash>>,
}

/// Mutable per-output state.
pub struct Output {
    pub config: OutputConfig,
    /// Never shared between outputs: an optimistic cache legitimately holds receipts from abandoned
    /// fork blocks, which would trip `store_receipt_to_tx`'s assert in another output.
    pub tx_cache: TxCache,
    /// Next height to consider.
    pub cursor: BlockHeight,
    /// Blocks at or below this are warm-up: already in Redis, so not re-emitted, and allowed to be
    /// missing tx hashes.
    pub redis_watermark: BlockHeight,
    pub expected_block_height: BlockHeight,
    pub last_block_height: Option<BlockHeight>,
    pub last_block_hash: Option<CryptoHash>,
    /// Refreshed once per sweep tick.
    pub head: BlockHeight,
    pub enabled: bool,
}

impl Output {
    pub fn new(config: OutputConfig, cursor: BlockHeight, redis_watermark: BlockHeight) -> Self {
        Self {
            config,
            tx_cache: TxCache::default(),
            cursor,
            redis_watermark,
            expected_block_height: cursor,
            last_block_height: None,
            last_block_hash: None,
            head: 0,
            enabled: true,
        }
    }
}

fn main() {
    #[allow(deprecated)]
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    let missing_receipts_whitelist: HashSet<_> = env::var("MISSING_RECEIPTS_WHITELIST")
        .map(|s| {
            s.split(',')
                .map(|s| {
                    s.parse::<CryptoHash>()
                        .expect("Failed to parse CryptoHash in MISSING_RECEIPTS_WHITELIST")
                })
                .collect()
        })
        .unwrap_or_default();
    let missing_receipts_whitelist = Arc::new(missing_receipts_whitelist);
    let receipt_backfill_depth = env::var("RECEIPT_BACKFILL_DEPTH")
        .map(|s| s.parse().unwrap())
        .unwrap_or(RECEIPT_BACKFILL_DEPTH);
    let finality: Finality =
        serde_json::from_str(&env::var("FINALITY").expect("Missing FINALITY env var"))
            .expect("Failed to parse Finality");
    let blocks_key = env::var("BLOCKS_KEY").expect("Missing BLOCKS_KEY env var");
    let stream_to_redis =
        env::var("STREAM_TO_REDIS").expect("Missing STREAM_TO_REDIS env var") == "true";
    let log_blocks = env::var("LOG_BLOCKS")
        .map(|s| s == "true")
        .unwrap_or(stream_to_redis);
    let max_num_blocks = env::var("MAX_NUM_BLOCKS").map(|s| s.parse().unwrap()).ok();
    let sweep_interval = Duration::from_millis(
        env::var("SWEEP_INTERVAL_MS")
            .map(|s| s.parse().unwrap())
            .unwrap_or(DEFAULT_SWEEP_INTERVAL_MS),
    );

    // Setting OPTIMISTIC_BLOCKS_KEY is what turns this into a two-stream node.
    let optimistic_blocks_key = env::var("OPTIMISTIC_BLOCKS_KEY")
        .ok()
        .filter(|s| !s.is_empty());
    if let Some(optimistic_blocks_key) = optimistic_blocks_key.as_ref() {
        assert_ne!(
            optimistic_blocks_key, &blocks_key,
            "BLOCKS_KEY and OPTIMISTIC_BLOCKS_KEY must differ. Pointing both sweeps at one stream \
             would silently drop half the blocks: the second write of each height comes back as \
             `The ID specified in XADD is equal or smaller ...`, which we treat as a benign duplicate."
        );
        assert_ne!(
            finality,
            Finality::None,
            "FINALITY must not be `optimistic` when OPTIMISTIC_BLOCKS_KEY is set, otherwise both \
             outputs would sweep the same head."
        );
    }
    let optimistic_max_num_blocks = env::var("OPTIMISTIC_MAX_NUM_BLOCKS")
        .map(|s| s.parse().unwrap())
        .ok()
        .or(max_num_blocks);
    let optimistic_enable_threshold = env::var("OPTIMISTIC_ENABLE_THRESHOLD")
        .map(|s| s.parse().unwrap())
        .unwrap_or(DEFAULT_OPTIMISTIC_ENABLE_THRESHOLD);
    let block_cache_window = env::var("BLOCK_CACHE_WINDOW")
        .map(|s| s.parse().unwrap())
        .unwrap_or(DEFAULT_BLOCK_CACHE_WINDOW);
    let block_cache_max_bytes = env::var("BLOCK_CACHE_MAX_BYTES")
        .map(|s| s.parse().unwrap())
        .unwrap_or(DEFAULT_BLOCK_CACHE_MAX_BYTES);

    // `RedisDB::new` hands the whole string to `Client::open`; only `caching-saver` splits on `,`.
    let redis_url = env::var("REDIS_URL").expect("Missing REDIS_URL env var");
    assert!(
        !redis_url.contains(','),
        "REDIS_URL must be a single URL for `node` (only `caching-saver` accepts a comma-separated list)"
    );

    let args: Vec<String> = std::env::args().collect();
    let home_dir = std::path::PathBuf::from(near_indexer::get_default_home());

    common::setup_tracing("redis=info,tokio_reactor=info,near=info,stats=info,telemetry=info,indexer=info,aggregated=info,redisnode=info");

    tracing::log::info!(target: PROJECT_ID, "Starting indexer");

    let command = args
        .get(1)
        .map(|arg| arg.as_str())
        .expect("You need to provide a command: `init` or `run` as arg");

    // Create or append file
    create_dir_all("res").expect("Failed to create res directory");
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("res/blocks_log.csv")
        .expect("Failed to create a log file");

    let start_block: Option<BlockHeight> = env::var("START_BLOCK").ok().map(|s| s.parse().unwrap());

    match command {
        "run" => {
            let sys = actix::System::new();
            sys.block_on(async move {
                let mut db = RedisDB::new(None).await.unwrap();
                let last_id = db.last_id(&blocks_key).await.unwrap();
                let mut last_redis_block_height: Option<BlockHeight> = last_id
                    .as_ref()
                    .map(|id| id.split_once("-").unwrap().0.parse().unwrap());
                let expected_block_height = if finality == Finality::None
                    || (start_block.is_none() && last_redis_block_height.is_none())
                {
                    // We are in optimistic mode, we need to stream closer to the last block in the redis.
                    tracing::log::info!(target: PROJECT_ID, "We are in optimistic mode or last_redis_block_height is none.");
                    let last_block_height = last_neardata_block_height().await;
                    let empty_redis_depth = if finality == Finality::None {
                        OPTIMISTIC_DEPTH
                    } else {
                        EMPTY_REDIS_DEPTH
                    };
                    last_redis_block_height = Some(last_block_height - empty_redis_depth);
                    last_redis_block_height.clone().unwrap() - receipt_backfill_depth - 1
                } else if let Some(last_redis_block_height) = last_redis_block_height {
                    tracing::log::info!(target: PROJECT_ID, "last_redis_block_height {}", last_redis_block_height);
                    last_redis_block_height + 1 - receipt_backfill_depth
                } else {
                    let start_block_height: BlockHeight = start_block.unwrap();
                    tracing::log::info!(target: PROJECT_ID, "start_block_height {}", start_block_height);
                    last_redis_block_height = Some(start_block_height - 1);

                    start_block_height - receipt_backfill_depth - 1
                };

                tracing::log::info!(target: PROJECT_ID, "Redis at block height: {:?}", last_redis_block_height);
                tracing::log::info!(target: PROJECT_ID, "Starting sweep at block height: {}", expected_block_height);

                let indexer_config = near_indexer::IndexerConfig {
                    home_dir,
                    // `sync_mode`, `finality` and `interval` only feed `streamer::start`, which we
                    // never call. Sweeping is driven by `Builder`/`Output` below.
                    sync_mode: near_indexer::SyncModeEnum::BlockHeight(expected_block_height),
                    await_for_node_synced: near_indexer::AwaitForNodeSyncedEnum::StreamWhileSyncing,
                    validate_genesis: false,
                    interval: sweep_interval,
                    finality: finality.clone(),
                };

                let indexer = near_indexer::Indexer::new(indexer_config).await.expect("Failed to create indexer");

                let mut outputs = vec![Output::new(
                    OutputConfig {
                        name: "final",
                        blocks_key,
                        finality,
                        max_num_blocks,
                        stream_to_redis,
                        log_blocks,
                        strict_hash_check: true,
                        fatal_on_redis_failure: true,
                        receipt_backfill_depth,
                        missing_receipts_whitelist: Arc::clone(&missing_receipts_whitelist),
                    },
                    expected_block_height,
                    last_redis_block_height.unwrap_or(0),
                )];

                if let Some(optimistic_blocks_key) = optimistic_blocks_key {
                    let mut optimistic = Output::new(
                        OutputConfig {
                            name: "optimistic",
                            blocks_key: optimistic_blocks_key,
                            finality: Finality::None,
                            max_num_blocks: optimistic_max_num_blocks,
                            stream_to_redis,
                            log_blocks,
                            // Streaming the chain head means seeing reorgs; that is the point.
                            strict_hash_check: false,
                            // A gap here must not take the final stream down with it.
                            fatal_on_redis_failure: false,
                            receipt_backfill_depth,
                            missing_receipts_whitelist: Arc::clone(&missing_receipts_whitelist),
                        },
                        0,
                        0,
                    );
                    // Anchored by `maybe_activate_optimistic` once the final output has caught up.
                    optimistic.enabled = false;
                    outputs.push(optimistic);
                }

                // `run` turns the window on only once two outputs are actually sweeping.
                let block_cache_window = if outputs.len() > 1 { block_cache_window } else { 0 };
                let builder = Builder::new(indexer, BlockCache::new(0, block_cache_max_bytes));

                run(
                    builder,
                    outputs,
                    db,
                    log_file,
                    sweep_interval,
                    optimistic_enable_threshold,
                    block_cache_window,
                )
                .await;

                actix::System::current().stop();
            });
            sys.run().unwrap();
        }
        _ => panic!("You have to pass `run` arg"),
    }
}

/// Sweep order: outputs that are up to date go first, so a backfill (notably the ~260 blocks the
/// optimistic output replays when it activates) never sits in front of somebody's live block.
/// Among those, lowest finality first, because `Finality::None` is the latency-sensitive stream.
///
/// This is only a preference. Both outputs share one symmetric `get_or_build`, so whichever
/// reaches a height first pays to build it and correctness never depends on the order.
fn priority_order(outputs: &[Output]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..outputs.len()).collect();
    order.sort_by_key(|&i| {
        let backfilling = outputs[i].head.saturating_sub(outputs[i].cursor) > BACKFILL_GAP;
        let finality_rank = match outputs[i].config.finality {
            Finality::None => 0,
            Finality::DoomSlug => 1,
            Finality::Final => 2,
        };
        (backfilling, finality_rank)
    });
    order
}

/// The optimistic stream has no completeness contract - it already re-anchors near the head on
/// every restart - so while the primary output has a real backlog we don't run it at all and spend
/// the whole machine on catching up. Capping work per tick would not help: the cost is
/// `build_streamer_message`, and at a 50ms interval any per-tick budget asks for more seconds of
/// work than there are seconds of wall clock.
///
/// Activation anchors on the node's *local* head rather than neardata. By this point the node is
/// caught up, so the local head is the better anchor, and it keeps startup off
/// `fetch_block_until_success`, which retries forever with no overall timeout - a neardata outage
/// would otherwise hang the whole process and neither stream would start.
async fn maybe_activate_optimistic(
    builder: &Builder,
    outputs: &mut [Output],
    threshold: u64,
    tick: u64,
) {
    if outputs.len() < 2 || outputs[1].enabled {
        return;
    }
    let gap = outputs[0].head.saturating_sub(outputs[0].cursor);
    if gap > threshold {
        if tick % STATS_EVERY_N_TICKS == 0 {
            tracing::log::info!(target: PROJECT_ID, "[{}] holding off, [{}] is {} blocks behind its head (threshold {})", outputs[1].config.name, outputs[0].config.name, gap, threshold);
        }
        return;
    }
    let Some(head) = builder.head(&outputs[1].config.finality).await else {
        return;
    };
    let watermark = head.saturating_sub(OPTIMISTIC_DEPTH);
    let cursor = watermark.saturating_sub(outputs[1].config.receipt_backfill_depth + 1);
    let optimistic = &mut outputs[1];
    optimistic.redis_watermark = watermark;
    optimistic.cursor = cursor;
    optimistic.expected_block_height = cursor;
    optimistic.last_block_height = None;
    optimistic.last_block_hash = None;
    optimistic.head = head;
    optimistic.enabled = true;
    tracing::log::info!(target: PROJECT_ID, "[{}] activated at head {}: sweeping from {}, emitting above {}", optimistic.config.name, head, cursor, watermark);
}

async fn run(
    mut builder: Builder,
    mut outputs: Vec<Output>,
    mut db: RedisDB,
    mut log_file: File,
    interval: Duration,
    optimistic_enable_threshold: u64,
    block_cache_window: u64,
) {
    debug_assert!(
        outputs.len() <= 2
            && (outputs.len() < 2 || outputs[1].config.finality == Finality::None),
        "outputs[0] is the primary stream and outputs[1], when present, is the optimistic one"
    );
    let mut tick: u64 = 0;
    loop {
        tokio::time::sleep(interval).await;
        tick += 1;

        for output in outputs.iter_mut() {
            if !output.enabled {
                continue;
            }
            if let Some(head) = builder.head(&output.config.finality).await {
                output.head = head;
            }
        }

        maybe_activate_optimistic(
            &builder,
            &mut outputs,
            optimistic_enable_threshold,
            tick,
        )
        .await;

        let enabled = outputs.iter().filter(|o| o.enabled).count();
        builder
            .cache
            .set_window(if enabled > 1 { block_cache_window } else { 0 });

        let keep_until = outputs
            .iter()
            .filter(|o| o.enabled)
            .map(|o| o.cursor)
            .min()
            .unwrap_or(0);
        builder.cache.prune_below(keep_until);

        for i in priority_order(&outputs) {
            if outputs[i].enabled {
                advance(&mut builder, &mut outputs[i], &mut db, &mut log_file, keep_until).await;
            }
        }

        if tick % STATS_EVERY_N_TICKS == 0 {
            let cursors = outputs
                .iter()
                .filter(|o| o.enabled)
                .map(|o| format!("{}={}/{}", o.config.name, o.cursor, o.head))
                .collect::<Vec<_>>()
                .join(" ");
            tracing::log::info!(target: PROJECT_ID, "Cursors {} cache {} blocks / {} bytes", cursors, builder.cache.len(), builder.cache.bytes());
            // Once the optimistic output is running we leave it running rather than flapping it,
            // but a backlog that re-opens means the machine is no longer keeping up.
            let gap = outputs[0].head.saturating_sub(outputs[0].cursor);
            if outputs.len() > 1 && outputs[1].enabled && gap > optimistic_enable_threshold {
                tracing::log::error!(target: PROJECT_ID, "[{}] is {} blocks behind its head (threshold {}) while [{}] is running", outputs[0].config.name, gap, optimistic_enable_threshold, outputs[1].config.name);
            }
        }
    }
}

/// Walks one output from its cursor up to the head it saw at the start of this tick.
///
/// The cursor only moves past a height that actually produced a block. Empty heights are still
/// stepped over within the sweep, but if the tail of the range is empty the cursor stays put and
/// those heights are re-read next tick. That matters at the tip: `fetch_latest_block` can return a
/// head whose height index is not readable yet, and retiring that height permanently would leave a
/// hole that trips the skipped-block assert on the very next block.
async fn advance(
    builder: &mut Builder,
    output: &mut Output,
    db: &mut RedisDB,
    log_file: &mut File,
    keep_until: BlockHeight,
) {
    let mut block_height = output.cursor;
    while block_height <= output.head {
        if let Some((_block_hash, bytes)) = builder.get_or_build(block_height, keep_until).await {
            emit_block(output, db, log_file, &bytes).await;
            output.cursor = block_height + 1;
        }
        block_height += 1;
    }
}

async fn emit_block(output: &mut Output, db: &mut RedisDB, log_file: &mut File, bytes: &[u8]) {
    // Bridges the two copies of `near_primitives` in the dependency graph (2.13.4 from the git
    // fork vs 0.37.4 from crates.io). Each output needs its own instance because
    // `BlockWithTxHashes` is not `Clone` and `process_block` mutates it in place.
    let fastnear_streamer_message: fastnear_primitives::near_indexer_primitives::StreamerMessage =
        serde_json::from_slice(bytes).unwrap();
    let mut block: BlockWithTxHashes = fastnear_streamer_message.into();
    let block_height = block.block.header.height;
    let prev_block_height = block.block.header.prev_height;
    if let Some(prev_block_height) = prev_block_height {
        assert!(
            output.expected_block_height > prev_block_height,
            "[{}] The indexer skipped a block. Expected block height: {}, but got: {}",
            output.config.name,
            output.expected_block_height,
            prev_block_height
        );
    }
    output.expected_block_height = block_height + 1;
    let prev_block_hash = block.block.header.prev_hash;
    if let Some(last_block_hash) = output.last_block_hash {
        if last_block_hash != prev_block_hash {
            let message = format!(
                "[{}] Block hashes don't match at block height: {}. Last block height {:?}, prev block height {:?}. Expected: {}, got: {}",
                output.config.name,
                block_height,
                output.last_block_height,
                prev_block_height,
                last_block_hash,
                prev_block_hash
            );
            if output.config.strict_hash_check {
                tracing::log::error!(target: PROJECT_ID, "{}", message);
                panic!("{}", message);
            } else {
                tracing::log::warn!(target: PROJECT_ID, "{}", message);
            }
        }
    }
    output.last_block_hash = Some(block.block.header.hash);
    let block_timestamp = block.block.header.timestamp;
    let current_time_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let time_diff_ns = current_time_ns.saturating_sub(block_timestamp);
    tracing::log::info!(target: PROJECT_ID, "[{}] Processing block {}\tlatency {:.3} sec", output.config.name, block_height, time_diff_ns as f64 / 1e9f64);
    let receipts_with_missing_tx_hashes =
        process_block(&mut output.tx_cache, &mut block, output.last_block_height);
    output.last_block_height = Some(block_height);

    let past_watermark = output.redis_watermark < block_height;

    if !receipts_with_missing_tx_hashes.is_empty() {
        let hashes_str = receipts_with_missing_tx_hashes
            .iter()
            .map(|h| h.receipt_id.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        tracing::log::warn!(target: PROJECT_ID, "[{}] Block {} is missing some tx hashes for receipts: [{}]", output.config.name, block_height, hashes_str);
        if past_watermark {
            if receipts_with_missing_tx_hashes
                .iter()
                .any(|r| !output.config.missing_receipts_whitelist.contains(&r.receipt_id))
            {
                tracing::log::error!(target: PROJECT_ID, "[{}] Block {} is missing some tx hashes for receipts: [{:?}]", output.config.name, block_height, receipts_with_missing_tx_hashes);
                panic!(
                    "[{}] Block {} is missing some tx hashes for receipts: [{}]",
                    output.config.name, block_height, hashes_str
                );
            }
        }
    }

    if !past_watermark || (!output.config.stream_to_redis && !output.config.log_blocks) {
        return;
    }

    let serialized_block = serde_json::to_string(&block).unwrap();
    let block_size = serialized_block.len();
    let block_local_hash = hash(serialized_block.as_bytes());
    let block_hash = block.block.header.hash;
    if output.config.log_blocks {
        writeln!(
            log_file,
            "{:?},{},{},{},{}",
            output.config.finality, block_height, block_hash, block_local_hash, block_size
        )
        .expect("Failed to write to log file");
    }

    if !output.config.stream_to_redis {
        return;
    }

    let data = vec![(BLOCK_KEY.to_string(), serialized_block)];

    let id = format!("{}-0", block_height);

    let mut delay = tokio::time::Duration::from_millis(INITIAL_RETRY_DELAY);
    for iter in 0..=MAX_RETRIES {
        if iter == MAX_RETRIES {
            let message = format!(
                "[{}] Failed to write block {} to redis after {} attempts",
                output.config.name, block_height, MAX_RETRIES
            );
            if output.config.fatal_on_redis_failure {
                panic!("{}. Don't want to skip the block", message);
            }
            // The optimistic stream tolerates gaps by construction, so don't take the final
            // stream down with it.
            tracing::log::error!(target: PROJECT_ID, "{}. Skipping the block", message);
            break;
        }
        let result = db
            .xadd(
                &output.config.blocks_key,
                &id,
                &data,
                output.config.max_num_blocks,
            )
            .await;
        match result {
            Ok(res) => {
                tracing::log::info!(target: PROJECT_ID, "[{}] Added {}", output.config.name, res);
                break;
            }
            Err(err) => {
                if err.kind() == redis::ErrorKind::ResponseError &&
                    err.to_string().contains("The ID specified in XADD is equal or smaller than the target stream top item") {
                    tracing::log::warn!(target: PROJECT_ID, "[{}] Duplicate ID {}: {}", output.config.name, id, err);
                    break;
                } else {
                    tracing::log::error!(target: PROJECT_ID, "[{}] Error: {}", output.config.name, err);
                    tokio::time::sleep(delay).await;
                    let _ = db.reconnect().await;
                    delay *= 2;
                    continue;
                }
            }
        }
    }
}

fn process_block(
    tx_cache: &mut TxCache,
    block: &mut BlockWithTxHashes,
    last_block_height: Option<BlockHeight>,
) -> Vec<ReceiptView> {
    let mut receipts_with_missing_tx_hashes = vec![];
    let mut receipt_hashes_to_remove = vec![];
    // Extract all tx_hashes first
    for shard in &block.shards {
        if let Some(chunk) = &shard.chunk {
            for txwo in &chunk.transactions {
                let tx_hash = txwo.transaction.hash;
                for receipt_id in &txwo.outcome.execution_outcome.outcome.receipt_ids {
                    tx_cache.store_receipt_to_tx(receipt_id, &tx_hash);
                }
            }
        }
    }
    // Finding all matching tx_hashes
    for shard in &mut block.shards {
        for reo in &mut shard.receipt_execution_outcomes {
            reo.tx_hash = tx_cache.peek_receipt_to_tx(&reo.receipt.receipt_id);
            receipt_hashes_to_remove.push(reo.receipt.receipt_id);
            if let Some(tx_hash) = reo.tx_hash {
                for receipt_id in &reo.execution_outcome.outcome.receipt_ids {
                    tx_cache.store_receipt_to_tx(receipt_id, &tx_hash);
                }
            } else {
                receipts_with_missing_tx_hashes.push(reo.receipt.clone());
            }
        }
    }

    let block_height = block.block.header.height;
    tx_cache.set_receipt_hashes_to_remove(block_height, receipt_hashes_to_remove);
    if let Some(last_block_height) = last_block_height {
        let diff = block_height.saturating_sub(last_block_height);
        for i in 0..=diff.min(RECEIPT_HASH_CLEANUP_BLOCKS) {
            tx_cache.clean_receipt_hashes_to_remove(
                last_block_height.saturating_sub(RECEIPT_HASH_CLEANUP_BLOCKS - i),
            );
        }
    }

    receipts_with_missing_tx_hashes
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block_hash(n: u8) -> BlockHash {
        BlockHash::hash_bytes(&[n])
    }

    fn bytes(len: usize) -> Arc<Vec<u8>> {
        Arc::new(vec![0u8; len])
    }

    fn output(name: &'static str, finality: Finality, cursor: BlockHeight, head: BlockHeight) -> Output {
        let mut o = Output::new(
            OutputConfig {
                name,
                blocks_key: name.to_string(),
                finality,
                max_num_blocks: None,
                stream_to_redis: false,
                log_blocks: false,
                strict_hash_check: true,
                fatal_on_redis_failure: true,
                receipt_backfill_depth: RECEIPT_BACKFILL_DEPTH,
                missing_receipts_whitelist: Arc::new(HashSet::new()),
            },
            cursor,
            0,
        );
        o.head = head;
        o
    }

    #[test]
    fn cache_admits_within_the_window_only() {
        let mut cache = BlockCache::new(64, usize::MAX);
        cache.admit(1000, block_hash(1), &bytes(10), 1000);
        cache.admit(1064, block_hash(2), &bytes(10), 1000);
        // One past the window.
        cache.admit(1065, block_hash(3), &bytes(10), 1000);
        assert_eq!(cache.len(), 2);
        assert!(cache.get(&block_hash(3)).is_none());
        assert_eq!(cache.bytes(), 20);
    }

    #[test]
    fn cache_is_inert_with_a_zero_window() {
        let mut cache = BlockCache::new(0, usize::MAX);
        cache.admit(1000, block_hash(1), &bytes(10), 1000);
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.bytes(), 0);
    }

    #[test]
    fn cache_stops_admitting_at_the_byte_backstop() {
        let mut cache = BlockCache::new(64, 100);
        cache.admit(1000, block_hash(1), &bytes(99), 1000);
        assert_eq!(cache.len(), 1);
        // Already at/over the cap, so nothing more is taken.
        cache.admit(1001, block_hash(2), &bytes(10), 1000);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.bytes(), 99);
    }

    #[test]
    fn prune_keeps_the_boundary_height() {
        let mut cache = BlockCache::new(64, usize::MAX);
        cache.admit(1000, block_hash(1), &bytes(10), 1000);
        cache.admit(1001, block_hash(2), &bytes(10), 1000);
        cache.admit(1002, block_hash(3), &bytes(10), 1000);
        cache.prune_below(1001);
        assert!(cache.get(&block_hash(1)).is_none());
        assert!(cache.get(&block_hash(2)).is_some());
        assert!(cache.get(&block_hash(3)).is_some());
        assert_eq!(cache.bytes(), 20);
    }

    /// A reorg leaves two hashes at one height. Both must be reachable while the height is live,
    /// and both must go when it is pruned - a one-to-one height index would leak the orphan.
    #[test]
    fn reorged_height_keeps_both_blocks_and_leaks_neither() {
        let mut cache = BlockCache::new(64, usize::MAX);
        let orphan = block_hash(1);
        let canonical = block_hash(2);
        cache.admit(1000, orphan, &bytes(10), 1000);
        cache.admit(1000, canonical, &bytes(10), 1000);
        assert_eq!(cache.len(), 2);
        assert!(cache.get(&orphan).is_some());
        assert!(cache.get(&canonical).is_some());

        cache.prune_below(1001);
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.bytes(), 0);
    }

    #[test]
    fn admitting_the_same_block_twice_does_not_double_count() {
        let mut cache = BlockCache::new(64, usize::MAX);
        cache.admit(1000, block_hash(1), &bytes(10), 1000);
        cache.admit(1000, block_hash(1), &bytes(10), 1000);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.bytes(), 10);
        cache.prune_below(1001);
        assert_eq!(cache.bytes(), 0);
    }

    #[test]
    fn closing_the_window_drops_everything() {
        let mut cache = BlockCache::new(64, usize::MAX);
        cache.admit(1000, block_hash(1), &bytes(10), 1000);
        cache.set_window(0);
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.bytes(), 0);
        cache.admit(1000, block_hash(1), &bytes(10), 1000);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn live_outputs_sweep_before_backfilling_ones() {
        // Steady state: both caught up, so the latency-sensitive optimistic stream leads.
        let outputs = vec![
            output("final", Finality::Final, 1000, 1000),
            output("optimistic", Finality::None, 1002, 1002),
        ];
        assert_eq!(priority_order(&outputs), vec![1, 0]);

        // Just activated: the optimistic backfill must not sit in front of a live final block.
        let outputs = vec![
            output("final", Finality::Final, 1000, 1001),
            output("optimistic", Finality::None, 740, 1001),
        ];
        assert_eq!(priority_order(&outputs), vec![0, 1]);
    }
}
