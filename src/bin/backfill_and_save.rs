mod block_with_tx_hash;
mod common;

use crate::block_with_tx_hash::BlockWithTxHashes;
use dotenv::dotenv;
use near_indexer::near_primitives::hash::CryptoHash;
use near_indexer::near_primitives::types::BlockHeight;
use near_indexer::streamer::build_streamer_message;
use near_indexer::{Indexer, StreamerMessage};
use std::collections::HashMap;
use std::time::Duration;
use std::{env, fs};
use tokio::sync::mpsc;

const PROJECT_ID: &str = "backfill";

const RECEIPT_BACKFILL_DEPTH: u64 = 250;
/// The number of blocks of receipts to keep in the cache before we start cleaning up.
/// It's necessary to keep receipts in memory for longer than one block in order to support
/// reorgs when streaming optimistic blocks.
const RECEIPT_HASH_CLEANUP_BLOCKS: u64 = 10;

pub type BlockHashes = Vec<CryptoHash>;

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

fn main() {
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    let save_every_n: u64 = 10;

    let tx_cache = TxCache::default();

    let home_dir = std::path::PathBuf::from(near_indexer::get_default_home());

    common::setup_tracing("tokio_reactor=info,near=info,stats=info,telemetry=info,indexer=info,aggregated=info,backfill=info");

    tracing::log::info!(target: PROJECT_ID, "Starting indexer");
    let indexer_config = near_indexer::IndexerConfig {
        home_dir,
        sync_mode: near_indexer::SyncModeEnum::FromInterruption,
        await_for_node_synced: near_indexer::AwaitForNodeSyncedEnum::StreamWhileSyncing,
        validate_genesis: false,
        interval: Duration::from_millis(500),
        finality: Default::default(),
    };

    let args = env::args().collect::<Vec<String>>();
    let start_block_height = args[1].parse::<BlockHeight>().unwrap();
    let end_block_height = args[2].parse::<BlockHeight>().unwrap();

    let save_path = args.get(3).cloned();

    let sys = actix::System::new();
    sys.block_on(async move {
        let near_config = indexer_config
            .load_near_config()
            .expect("failed to load near config");
        let near_node = Indexer::start_near_node(&indexer_config, near_config.clone())
            .await
            .expect("failed to start near node");
        let indexer = Indexer::from_near_node(indexer_config, near_config, &near_node);
        let stream = streamer(indexer, start_block_height, end_block_height);
        listen_blocks(
            stream,
            tx_cache,
            save_path,
            save_every_n,
            start_block_height,
        )
        .await;

        actix::System::current().stop();
    });
    sys.run().unwrap();
}
async fn start(
    indexer: Indexer,
    block_sink: mpsc::Sender<StreamerMessage>,
    start_block_height: BlockHeight,
    end_block_height: BlockHeight,
) {
    // Reading the input file
    let view_client = indexer.view_client.clone();
    for block_height in start_block_height - RECEIPT_BACKFILL_DEPTH..end_block_height {
        let block = view_client.fetch_block_by_height(block_height).await;
        if let Ok(Some(block)) = block {
            let response =
                build_streamer_message(&view_client, block, &indexer.shard_tracker).await;
            if let Ok(response) = response {
                block_sink.send(response).await.unwrap();
            } else {
                tracing::warn!(target: PROJECT_ID, "Failed to build block {}", block_height);
            }
        } else {
            tracing::warn!(target: PROJECT_ID, "Failed to fetch block {}", block_height);
        }
    }
}

fn streamer(
    indexer: Indexer,
    start_block_height: BlockHeight,
    end_block_height: BlockHeight,
) -> mpsc::Receiver<StreamerMessage> {
    let (sender, receiver) = mpsc::channel(100);
    actix::spawn(start(indexer, sender, start_block_height, end_block_height));
    receiver
}

fn save_blocks(
    blocks: &[(BlockHeight, String)],
    path: Option<&String>,
    save_every_n: u64,
) -> std::io::Result<()> {
    if blocks.is_empty() || path.is_none() {
        return Ok(());
    }
    let starting_block = blocks[0].0 / save_every_n * save_every_n;
    let padded_block_height = format!("{:0>12}", starting_block);
    let path = format!(
        "{}/{}/{}",
        path.unwrap(),
        &padded_block_height[..6],
        &padded_block_height[6..9]
    );

    fs::create_dir_all(&path)?;
    let filename = format!("{}/{}.tgz", path, padded_block_height);
    tracing::info!(target: PROJECT_ID, "Saving blocks to: {}", filename);
    // Creating archive
    let tar_gz = fs::File::create(&filename)?;
    let enc = flate2::write::GzEncoder::new(tar_gz, flate2::Compression::default());
    let mut tar = tar::Builder::new(enc);
    for (block_height, block_str) in blocks {
        let padded_block_height = format!("{:0>12}", block_height);
        let mut header = tar::Header::new_gnu();
        header.set_path(&format!("{}.json", padded_block_height))?;
        header.set_size(block_str.len() as u64);
        header.set_cksum();
        tar.append(&header, block_str.as_bytes())?;
    }

    tar.into_inner()?.finish()?;
    Ok(())
}

async fn listen_blocks(
    mut stream: mpsc::Receiver<StreamerMessage>,
    mut tx_cache: TxCache,
    save_path: Option<String>,
    save_every_n: u64,
    start_block_height: BlockHeight,
) {
    let mut blocks = vec![];
    let mut last_block_height = None;

    while let Some(streamer_message) = stream.recv().await {
        let block_height = streamer_message.block.header.height;
        tracing::info!(target: PROJECT_ID, "Processing block: {}", block_height);

        let mut block: BlockWithTxHashes = streamer_message.into();
        let has_all_tx_hashes = process_block(&mut tx_cache, &mut block, last_block_height);
        last_block_height = Some(block_height);

        if !has_all_tx_hashes {
            tracing::log::warn!(target: PROJECT_ID, "Block {} is missing some tx hashes", block_height);
            if start_block_height < block_height {
                panic!("Block {} is missing some tx hashes", block_height);
            }
        }

        if start_block_height >= block_height {
            continue;
        }

        if blocks.get(0).map(|(height, _block)| *height).unwrap_or(0) / save_every_n
            != block_height / save_every_n
        {
            save_blocks(&blocks, save_path.as_ref(), save_every_n).unwrap();
            blocks.clear();
        }
        // Update cache
        blocks.push((block_height, serde_json::to_string(&block).unwrap()));
    }

    if !blocks.is_empty() {
        save_blocks(&blocks, save_path.as_ref(), save_every_n).unwrap();
    }
}
