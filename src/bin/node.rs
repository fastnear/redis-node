mod common;
mod db;

use db::DB;
use dotenv::dotenv;

const PROJECT_ID: &str = "redisnode";
const MAX_NUM_BLOCKS: Option<usize> = Some(1000000);

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
            let indexer_config = near_indexer::IndexerConfig {
                home_dir,
                sync_mode: near_indexer::SyncModeEnum::FromInterruption,
                await_for_node_synced: near_indexer::AwaitForNodeSyncedEnum::StreamWhileSyncing,
                validate_genesis: false,
            };
            let sys = actix::System::new();
            sys.block_on(async move {
                let db = DB::new().await;
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
    mut db: DB,
) {
    while let Some(streamer_message) = stream.recv().await {
        let data = vec![(
            "block".to_string(),
            serde_json::to_string(&streamer_message).unwrap(),
        )];

        // TODO: Retry
        let id = format!("{}-0", streamer_message.block.header.height);
        let result = db.xadd("blocks", &id, &data, MAX_NUM_BLOCKS).await;
        match result {
            Ok(res) => {
                tracing::log::debug!(target: PROJECT_ID, "Added {}", res);
            }
            Err(res) => {
                if res.kind() == redis::ErrorKind::ResponseError {
                    tracing::log::debug!(target: PROJECT_ID, "Duplicate ID");
                } else {
                    panic!("Error: {}", res);
                }
            }
        }
    }
}
