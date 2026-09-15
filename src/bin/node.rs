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
            || self.bytes >= self.max_bytes
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

                let output = Output::new(
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
                        missing_receipts_whitelist,
                    },
                    expected_block_height,
                    last_redis_block_height.unwrap_or(0),
                );

                // A single output never benefits from the cache, so prove it is a no-op.
                let builder = Builder::new(indexer, BlockCache::new(0, 0));

                run(builder, vec![output], db, log_file, sweep_interval).await;

                actix::System::current().stop();
            });
            sys.run().unwrap();
        }
        _ => panic!("You have to pass `run` arg"),
    }
}

/// Outputs to sweep, lowest finality first. `Finality::None` is latency-sensitive, so it goes
/// first - but this is only a preference. Both outputs share one symmetric `get_or_build`, so
/// whichever reaches a height first pays for it and correctness never depends on the order.
fn priority_order(outputs: &[Output]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..outputs.len()).collect();
    order.sort_by_key(|&i| match outputs[i].config.finality {
        Finality::None => 0,
        Finality::DoomSlug => 1,
        Finality::Final => 2,
    });
    order
}

async fn run(
    mut builder: Builder,
    mut outputs: Vec<Output>,
    mut db: RedisDB,
    mut log_file: File,
    interval: Duration,
) {
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
                .map(|o| format!("{}={}/{}", o.config.name, o.cursor, o.head))
                .collect::<Vec<_>>()
                .join(" ");
            tracing::log::info!(target: PROJECT_ID, "Cursors {} cache {} blocks / {} bytes", cursors, builder.cache.len(), builder.cache.bytes());
        }
    }
}

/// Walks one output from its cursor up to the head it saw at the start of this tick.
async fn advance(
    builder: &mut Builder,
    output: &mut Output,
    db: &mut RedisDB,
    log_file: &mut File,
    keep_until: BlockHeight,
) {
    while output.cursor <= output.head {
        let block_height = output.cursor;
        if let Some((_block_hash, bytes)) = builder.get_or_build(block_height, keep_until).await {
            emit_block(output, db, log_file, &bytes).await;
        }
        output.cursor = block_height + 1;
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
