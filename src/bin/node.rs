mod common;
mod redis_db;

use dotenv::dotenv;
use near_indexer::near_primitives::types::BlockHeight;
use redis_db::RedisDB;
use std::env;

const PROJECT_ID: &str = "redisnode";
const FINAL_BLOCKS_KEY: &str = "final_blocks";
const BLOCK_KEY: &str = "block";

const MAX_RETRIES: usize = 10;
const INITIAL_RETRY_DELAY: u64 = 100;

fn main() {
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    let args: Vec<String> = std::env::args().collect();
    let home_dir = std::path::PathBuf::from(near_indexer::get_default_home());

    common::setup_tracing("redis=info,tokio_reactor=info,near=info,stats=info,telemetry=info,indexer=info,aggregated=info,redisnode=info");

    tracing::log::info!(target: PROJECT_ID, "Starting indexer");

    let command = args
        .get(1)
        .map(|arg| arg.as_str())
        .expect("You need to provide a command: `init` or `run` as arg");

    match command {
        "run" => {
            let sys = actix::System::new();
            sys.block_on(async move {
                let mut db = RedisDB::new().await;
                let last_id = db.last_id(FINAL_BLOCKS_KEY).await.unwrap();
                let sync_mode = if let Some(last_id) = last_id {
                    let last_block_height: BlockHeight =
                        last_id.split_once("-").unwrap().0.parse().unwrap();
                    near_indexer::SyncModeEnum::BlockHeight(last_block_height + 1)
                } else if env::var("START_BLOCK").is_ok() {
                    near_indexer::SyncModeEnum::BlockHeight(
                        env::var("START_BLOCK").unwrap().parse().unwrap(),
                    )
                } else {
                    near_indexer::SyncModeEnum::FromInterruption
                };

                let indexer_config = near_indexer::IndexerConfig {
                    home_dir,
                    sync_mode,
                    await_for_node_synced: near_indexer::AwaitForNodeSyncedEnum::StreamWhileSyncing,
                    validate_genesis: false,
                };

                let indexer = near_indexer::Indexer::new(indexer_config).unwrap();
                let stream = indexer.streamer();
                listen_blocks(stream, db).await;

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
) {
    let max_num_blocks = env::var("MAX_NUM_BLOCKS").map(|s| s.parse().unwrap()).ok();

    while let Some(streamer_message) = stream.recv().await {
        let data = vec![(
            BLOCK_KEY.to_string(),
            serde_json::to_string(&streamer_message).unwrap(),
        )];

        let id = format!("{}-0", streamer_message.block.header.height);

        let mut delay = tokio::time::Duration::from_millis(INITIAL_RETRY_DELAY);
        for _ in 0..MAX_RETRIES {
            let result = db.xadd(FINAL_BLOCKS_KEY, &id, &data, max_num_blocks).await;
            match result {
                Ok(res) => {
                    tracing::log::debug!(target: PROJECT_ID, "Added {}", res);
                }
                Err(err) => {
                    if err.kind() == redis::ErrorKind::ResponseError {
                        tracing::log::debug!(target: PROJECT_ID, "Duplicate ID");
                    } else {
                        tracing::log::error!(target: PROJECT_ID, "Error: {}", err);
                        tokio::time::sleep(delay).await;
                        let _ = db.reconnect().await;
                        delay *= 2;
                        continue;
                    }
                }
            }
            break;
        }
    }
}
