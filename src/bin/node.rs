mod block_with_tx_hash;
mod common;
mod redis_db;

use crate::block_with_tx_hash::BlockWithTxHashes;
use dotenv::dotenv;
use fastnear_neardata_fetcher::fetcher;
use near_indexer::near_primitives::hash::CryptoHash;
use near_indexer::near_primitives::types::{BlockHeight, Finality};
use redis_db::RedisDB;
use std::collections::HashMap;
use std::env;

pub type BlockHashes = Vec<CryptoHash>;

const PROJECT_ID: &str = "redisnode";
const BLOCK_KEY: &str = "block";

const MAX_RETRIES: usize = 10;
const INITIAL_RETRY_DELAY: u64 = 100;
const RECEIPT_BACKFILL_DEPTH: u64 = 250;
const EMPTY_REDIS_DEPTH: u64 = 1000;

/// The number of blocks of receipts to keep in the cache before we start cleaning up.
/// It's necessary to keep receipts in memory for longer than one block in order to support
/// reorgs when streaming optimistic blocks.
const RECEIPT_HASH_CLEANUP_BLOCKS: u64 = 10;

pub struct Config {
    pub stream_to_redis: bool,
    pub max_num_blocks: Option<usize>,
    pub blocks_key: String,
    pub finality: Finality,
}

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

fn main() {
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    let receipt_backfill_depth = env::var("RECEIPT_BACKFILL_DEPTH")
        .map(|s| s.parse().unwrap())
        .unwrap_or(RECEIPT_BACKFILL_DEPTH);
    let finality: Finality =
        serde_json::from_str(&env::var("FINALITY").expect("Missing FINALITY env var"))
            .expect("Failed to parse Finality");
    let blocks_key = env::var("BLOCKS_KEY").expect("Missing BLOCKS_KEY env var");

    let tx_cache = TxCache::default();

    let args: Vec<String> = std::env::args().collect();
    let home_dir = std::path::PathBuf::from(near_indexer::get_default_home());

    common::setup_tracing("redis=info,tokio_reactor=info,near=info,stats=info,telemetry=info,indexer=info,aggregated=info,redisnode=info");

    tracing::log::info!(target: PROJECT_ID, "Starting indexer");

    let command = args
        .get(1)
        .map(|arg| arg.as_str())
        .expect("You need to provide a command: `init` or `run` as arg");

    let config = Config {
        stream_to_redis: env::var("STREAM_TO_REDIS").expect("Missing STREAM_TO_REDIS env var")
            == "true",
        max_num_blocks: env::var("MAX_NUM_BLOCKS").map(|s| s.parse().unwrap()).ok(),
        blocks_key,
        finality,
    };

    match command {
        "run" => {
            let sys = actix::System::new();
            sys.block_on(async move {
                let mut db = RedisDB::new(None).await.unwrap();
                let last_id = db.last_id(&config.blocks_key).await.unwrap();
                let last_tx_cache_block = 0;
                let mut last_redis_block_height: Option<BlockHeight> = last_id
                    .as_ref()
                    .map(|id| id.split_once("-").unwrap().0.parse().unwrap());
                let sync_mode = if let Some(last_redis_block_height) = last_redis_block_height {
                    // There is data in the redis. We need to stream closer to this height.
                    let next_block = last_redis_block_height + 1;
                    if last_tx_cache_block > next_block {
                        // Have to backfill
                        near_indexer::SyncModeEnum::BlockHeight(next_block - receipt_backfill_depth)
                    } else {
                        // Backfill or catch up
                        near_indexer::SyncModeEnum::BlockHeight(
                            last_tx_cache_block.max(next_block - receipt_backfill_depth),
                        )
                    }
                } else if env::var("START_BLOCK").is_ok() {
                    let start_block_height: BlockHeight =
                        env::var("START_BLOCK").unwrap().parse().unwrap();
                    last_redis_block_height = Some(start_block_height - 1);
                    near_indexer::SyncModeEnum::BlockHeight(
                        start_block_height - receipt_backfill_depth - 1,
                    )
                } else {
                    let client = reqwest::Client::new();
                    let chain_id = fastnear_primitives::types::ChainId::try_from(
                        env::var("CHAIN_ID").expect("CHAIN_ID is not set"),
                    )
                    .expect("Invalid chain id");
                    let last_block_height = fetcher::fetch_last_block(&client, chain_id)
                        .await
                        .expect("Last block doesn't exists")
                        .block
                        .header
                        .height;
                    last_redis_block_height = Some(last_block_height - EMPTY_REDIS_DEPTH);
                    near_indexer::SyncModeEnum::BlockHeight(
                        last_redis_block_height.clone().unwrap() - receipt_backfill_depth - 1,
                    )
                };

                let indexer_config = near_indexer::IndexerConfig {
                    home_dir,
                    sync_mode,
                    await_for_node_synced: near_indexer::AwaitForNodeSyncedEnum::StreamWhileSyncing,
                    validate_genesis: false,
                    interval: std::time::Duration::from_millis(50),
                    finality: config.finality.clone(),
                };

                let indexer = near_indexer::Indexer::new(indexer_config).unwrap();
                let stream = indexer.streamer();
                listen_blocks(stream, db, tx_cache, config, last_redis_block_height).await;

                actix::System::current().stop();
            });
            sys.run().unwrap();
        }
        _ => panic!("You have to pass `run` arg"),
    }
}

async fn listen_blocks(
    mut stream: tokio::sync::mpsc::Receiver<near_indexer::StreamerMessage>,
    mut db: RedisDB,
    mut tx_cache: TxCache,
    config: Config,
    last_redis_block_height: Option<BlockHeight>,
) {
    let redis_block = last_redis_block_height.unwrap_or(0);
    let mut last_block_height = None;
    while let Some(streamer_message) = stream.recv().await {
        let block_height = streamer_message.block.header.height;
        tracing::log::info!(target: PROJECT_ID, "Processing block {}", block_height);
        let mut block: BlockWithTxHashes = streamer_message.into();
        let has_all_tx_hashes = process_block(&mut tx_cache, &mut block, last_block_height);
        last_block_height = Some(block_height);

        if !has_all_tx_hashes {
            tracing::log::warn!(target: PROJECT_ID, "Block {} is missing some tx hashes", block_height);
            if redis_block < block_height {
                panic!("Block {} is missing some tx hashes", block_height);
            }
        }

        if !config.stream_to_redis || redis_block >= block_height {
            continue;
        }

        let data = vec![(
            BLOCK_KEY.to_string(),
            serde_json::to_string(&block).unwrap(),
        )];

        let id = format!("{}-0", block_height);

        let mut delay = tokio::time::Duration::from_millis(INITIAL_RETRY_DELAY);
        for iter in 0..=MAX_RETRIES {
            if iter == MAX_RETRIES {
                panic!("Failed to write to redis. Don't want to skip the block");
            }
            let result = db
                .xadd(&config.blocks_key, &id, &data, config.max_num_blocks)
                .await;
            match result {
                Ok(res) => {
                    tracing::log::info!(target: PROJECT_ID, "Added {}", res);
                    break;
                }
                Err(err) => {
                    if err.kind() == redis::ErrorKind::ResponseError &&
                        err.to_string().contains("The ID specified in XADD is equal or smaller than the target stream top item") {
                        tracing::log::warn!(target: PROJECT_ID, "Duplicate ID {}: {}", id, err);
                        break;
                    } else {
                        tracing::log::error!(target: PROJECT_ID, "Error: {}", err);
                        tokio::time::sleep(delay).await;
                        let _ = db.reconnect().await;
                        delay *= 2;
                        continue;
                    }
                }
            }
        }
    }
}

fn process_block(
    tx_cache: &mut TxCache,
    block: &mut BlockWithTxHashes,
    last_block_height: Option<BlockHeight>,
) -> bool {
    let mut has_all_tx_hashes = true;
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
                has_all_tx_hashes = false;
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

    has_all_tx_hashes
}
