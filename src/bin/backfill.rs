mod block_with_tx_hash;
mod common;

use anyhow::Context;
use dotenv::dotenv;
use near_indexer::near_primitives::types::BlockHeight;
use near_indexer::streamer::build_streamer_message;
use near_indexer::streamer::fetchers::fetch_block_by_height;
use near_indexer::{Indexer, StreamerMessage};
use std::io::Write;
use std::{env, fs};
use tokio::sync::mpsc;

const PROJECT_ID: &str = "backfill";

fn main() {
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    let home_dir = std::path::PathBuf::from(near_indexer::get_default_home());

    common::setup_tracing("tokio_reactor=info,near=info,stats=info,telemetry=info,indexer=info,aggregated=info,backfill=info");

    let save_path = env::var("DATA_PATH").expect("DATA_PATH is not set");

    tracing::log::info!(target: PROJECT_ID, "Starting indexer");
    let indexer_config = near_indexer::IndexerConfig {
        home_dir,
        sync_mode: near_indexer::SyncModeEnum::FromInterruption,
        await_for_node_synced: near_indexer::AwaitForNodeSyncedEnum::StreamWhileSyncing,
        validate_genesis: false,
        interval: Default::default(),
        finality: Default::default(),
    };

    let sys = actix::System::new();
    sys.block_on(async move {
        let indexer = near_indexer::Indexer::new(indexer_config).unwrap();
        let stream = streamer(indexer);
        listen_blocks(stream, &save_path).await;

        actix::System::current().stop();
    });
    sys.run().unwrap();
}

fn read_missing_blocks() -> Vec<BlockHeight> {
    std::fs::read_to_string(
        env::var("MISSING_BLOCKS_FILE").expect("MISSING_BLOCKS_FILE is not set"),
    )
    .context("Failed to read the input file")
    .unwrap()
    .trim()
    .split("\n")
    .map(|s| {
        let b = s
            .trim()
            .split("-")
            .map(|s| {
                s.trim()
                    .parse::<BlockHeight>()
                    .expect("Failed to parse block height")
            })
            .collect::<Vec<_>>();
        (b[0] + 1..=b[1]).collect::<Vec<_>>()
    })
    .flatten()
    .collect::<Vec<_>>()
}

async fn start(indexer: Indexer, block_sink: mpsc::Sender<StreamerMessage>) {
    // Reading the input file
    let missing_blocks = read_missing_blocks();
    let view_client = indexer.view_client.clone();
    for block_height in missing_blocks {
        let block = fetch_block_by_height(&view_client, block_height).await;
        if let Ok(block) = block {
            let response = build_streamer_message(&view_client, block).await;
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

fn streamer(indexer: Indexer) -> mpsc::Receiver<StreamerMessage> {
    let (sender, receiver) = mpsc::channel(100);
    actix::spawn(start(indexer, sender));
    receiver
}

fn save_block(path: &str, block: StreamerMessage) -> std::io::Result<()> {
    let block_height = block.block.header.height;
    let value = serde_json::to_string(&block).unwrap();
    let padded_block_height = format!("{:0>12}", block_height);
    let block_dir = format!(
        "{}/{}/{}",
        path,
        &padded_block_height[..6],
        &padded_block_height[6..9]
    );
    let block_file = format!("{}/{}.json", block_dir, padded_block_height);
    tracing::info!(target: PROJECT_ID, "Saving {} bytes to file {}", value.len(), block_file);
    fs::create_dir_all(&block_dir).unwrap();
    let mut file = fs::File::create(&block_file).unwrap();
    file.write_all(value.as_bytes()).unwrap();
    Ok(())
}

async fn listen_blocks(mut stream: mpsc::Receiver<StreamerMessage>, save_path: &str) {
    while let Some(streamer_message) = stream.recv().await {
        let block_height = streamer_message.block.header.height;
        tracing::log::info!(target: PROJECT_ID, "Processing block {}", block_height);
        save_block(save_path, streamer_message).unwrap();
    }
}
