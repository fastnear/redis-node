mod common;
mod redis_db;

use dotenv::dotenv;
use futures::future::join_all;
use redis::aio::MultiplexedConnection;
use redis::RedisResult;
use redis_db::RedisDB;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use std::{env, fs, process};
use tokio::sync::mpsc;

pub type BlockHeight = u64;

const PROJECT_ID: &str = "saver";
const SAFE_OFFSET: u64 = 30;

const BLOCK_KEY: &str = "block";
const DEFAULT_CACHE_EXPIRATION: std::time::Duration = std::time::Duration::from_secs(60);

fn read_folder(path: &String) -> Vec<String> {
    let mut entries: Vec<String> = fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
        .collect();
    entries.sort();
    entries
}

fn last_block_height(path: &String) -> Option<BlockHeight> {
    let last_outer = read_folder(path).last().cloned()?;
    let last_thousand = read_folder(&format!("{}/{}", path, last_outer))
        .last()
        .cloned()?;
    let last_file = read_folder(&format!("{}/{}/{}", path, last_outer, last_thousand))
        .last()
        .cloned()?;
    last_file.split_once(".").unwrap().0.parse().ok()
}

async fn first_block_id(read_db: &mut RedisDB, blocks_key: &str) -> Option<BlockHeight> {
    let id = read_db.first_id(blocks_key).await.ok()??;
    Some(id.split_once("-").unwrap().0.parse().unwrap())
}

async fn last_block_id(read_db: &mut RedisDB, blocks_key: &str) -> Option<BlockHeight> {
    let id = read_db.last_id(blocks_key).await.ok()??;
    Some(id.split_once("-").unwrap().0.parse().unwrap())
}

async fn block_producer(
    mut redis_db: RedisDB,
    blocks_key: String,
    sink: mpsc::Sender<(BlockHeight, String)>,
    last_processed_block: Arc<RwLock<BlockHeight>>,
) {
    let mut last_id = format!("{}-0", *last_processed_block.read().unwrap());
    loop {
        let res = redis_db.xread(1, &blocks_key, &last_id).await;
        let res = match res {
            Ok(res) => res,
            Err(err) => {
                tracing::error!(target: PROJECT_ID, "Error: {}", err);
                tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
                let _ = redis_db.reconnect().await;
                continue;
            }
        };
        if res.is_empty() {
            tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
            continue;
        }
        let (id, key_values) = res.into_iter().next().unwrap();
        assert_eq!(key_values.len(), 1, "Expected 1 key-value pair");
        let (key, value) = key_values.into_iter().next().unwrap();
        assert_eq!(key, BLOCK_KEY, "Expected key to be block");
        let block_height: BlockHeight = id.split_once("-").unwrap().0.parse().unwrap();
        tracing::debug!(target: PROJECT_ID, "Adding block {} from redis {:?}", block_height, redis_db.client.get_connection_info().addr);
        sink.send((block_height, value)).await.unwrap();
        let current_processed_block = *last_processed_block.read().unwrap();
        if current_processed_block > block_height {
            last_id = format!("{}-0", current_processed_block);
        } else {
            last_id = id;
        }
    }
}

#[tokio::main]
async fn main() {
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    let blocks_key = env::var("BLOCKS_KEY").unwrap_or("final_blocks".to_string());

    let save_every_n = env::var("SAVE_EVERY_N")
        .expect("SAVE_EVERY_N is not set")
        .parse()
        .unwrap();

    let cache_expiration = env::var("CACHE_EXPIRATION")
        .ok()
        .map(|s| Duration::from_secs(s.parse::<u64>().unwrap()))
        .unwrap_or(DEFAULT_CACHE_EXPIRATION);

    let chain_id = env::var("CHAIN_ID").expect("CHAIN_ID is not set");
    common::setup_tracing("redis=info,saver=debug");
    let redis_urls: Vec<_> = env::var("REDIS_URL")
        .expect("REDIS_URL is not set")
        .split(',')
        .map(|s| s.to_string())
        .collect();
    let mut read_dbs = vec![];
    for url in &redis_urls {
        match RedisDB::new(Some(url.to_string())).await {
            Ok(db) => read_dbs.push(db),
            Err(e) => {
                tracing::error!(target: PROJECT_ID, "Failed to connect to Redis ({}): {}", url, e);
            }
        }
    }

    if read_dbs.is_empty() {
        panic!("No Redis connections available");
    }

    let path = env::var("ARCHIVAL_DATA_PATH").ok();
    let last_block_height = path.as_ref().and_then(last_block_height);

    let first_block_heights = join_all(
        read_dbs
            .iter_mut()
            .map(|db| first_block_id(db, &blocks_key))
            .collect::<Vec<_>>(),
    )
    .await;
    let first_block_height = first_block_heights
        .iter()
        .filter_map(|h| *h)
        .max()
        .expect("No blocks found in Redis");

    let last_available_block_heights = join_all(
        read_dbs
            .iter_mut()
            .map(|db| last_block_id(db, &blocks_key))
            .collect::<Vec<_>>(),
    )
    .await;
    let last_available_block_height = last_available_block_heights
        .iter()
        .filter_map(|h| *h)
        .max()
        .expect("No blocks found in Redis");

    let min_start_block: BlockHeight =
        (first_block_height + SAFE_OFFSET + save_every_n) / save_every_n * save_every_n;

    let mut start_block = last_block_height
        .unwrap_or(last_available_block_height.saturating_sub(SAFE_OFFSET))
        / save_every_n
        * save_every_n;

    if start_block < first_block_height + SAFE_OFFSET {
        tracing::warn!(target: PROJECT_ID, "Start block is too early, resetting to {}", min_start_block);
        start_block = min_start_block;
    }

    let last_processed_block = Arc::new(RwLock::new(start_block.saturating_sub(1)));

    tracing::info!(target: PROJECT_ID, "Resuming from {}", last_processed_block.read().unwrap());

    let mut cache_db = if let Some(cache_url) = env::var("CACHE_REDIS_URL").ok() {
        tracing::info!(target: PROJECT_ID, "Connecting to cache redis");
        Some(
            RedisDB::new(Some(cache_url))
                .await
                .expect("Failed to connect to cache redis"),
        )
    } else {
        tracing::info!(target: PROJECT_ID, "No cache redis URL provided");
        None
    };

    let (sender, mut receiver) = mpsc::channel(100);
    for db in read_dbs {
        let blocks_key = blocks_key.clone();
        let sender = sender.clone();
        let last_processed_block = last_processed_block.clone();
        tokio::spawn(block_producer(db, blocks_key, sender, last_processed_block));
    }

    let mut last_block_height: BlockHeight = 0;
    let mut blocks = vec![];
    while let Some((block_height, value)) = receiver.recv().await {
        if last_block_height > 0 && block_height <= last_block_height {
            continue;
        }
        tracing::debug!(target: PROJECT_ID, "Processing block: {}", block_height);
        *last_processed_block.write().unwrap() = block_height;

        if blocks.get(0).map(|(height, _block)| *height).unwrap_or(0) / save_every_n
            != block_height / save_every_n
        {
            let mut new_blocks = vec![];
            std::mem::swap(&mut blocks, &mut new_blocks);
            save_blocks_async(new_blocks, path.clone(), save_every_n);
        }
        // Update cache

        let mut blocks_to_update = vec![(block_height, value.as_str())];
        if block_height > last_block_height && block_height - last_block_height < 10 {
            for i in last_block_height + 1..block_height {
                tracing::debug!(target: PROJECT_ID, "Filling gap at skipped block: {}", i);
                blocks_to_update.push((i, ""));
            }
        }
        if let Some(cache_db) = cache_db.as_mut() {
            with_retries!(cache_db, |connection| async {
                set_block_and_last_block_height(
                    connection,
                    &chain_id,
                    block_height,
                    &blocks_to_update,
                    cache_expiration,
                )
                .await
            })
            .expect("Failed to set last block height in cache");
        }

        blocks.push((block_height, value));
        last_block_height = block_height;
    }
}

fn save_blocks_async(blocks: Vec<(BlockHeight, String)>, path: Option<String>, save_every_n: u64) {
    if blocks.is_empty() || path.is_none() {
        return;
    }
    tokio::spawn((|| async move {
        save_blocks(&blocks, path, save_every_n)
            .map_err(|e| {
                tracing::error!(target: PROJECT_ID, "Failed to save blocks: {}", e);
                process::exit(1);
            })
            .unwrap();
    })());
}

fn save_blocks(
    blocks: &[(BlockHeight, String)],
    path: Option<String>,
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

pub(crate) async fn set_block_and_last_block_height(
    connection: &mut MultiplexedConnection,
    chain_id: &str,
    last_block_height: BlockHeight,
    blocks: &[(BlockHeight, &str)],
    cache_expiration: Duration,
) -> Result<(), redis::RedisError> {
    let mut pipe = redis::pipe();

    for (block_height, block) in blocks {
        pipe.cmd("SET")
            .arg(format!("b:{}:{}", chain_id, block_height))
            .arg(block)
            .arg("EX")
            .arg(cache_expiration.as_secs())
            .ignore();
    }

    let _res: () = pipe
        .cmd("SET")
        .arg(format!("meta:{}:last_block", chain_id))
        .arg(last_block_height)
        .ignore()
        .query_async(connection)
        .await?;

    let res: RedisResult<redis::Value> = redis::cmd("XADD")
        .arg(format!("meta:{}:last_blocks_queue", chain_id))
        .arg("MAXLEN")
        .arg("~")
        .arg(10)
        .arg(format!("{}-0", last_block_height))
        .arg(&["a", "b"])
        .query_async(connection)
        .await;

    match res {
        Ok(_) => Ok(()),
        Err(err) => {
            if err.kind() == redis::ErrorKind::ResponseError
                && err.to_string().contains(
                    "The ID specified in XADD is equal or smaller than the target stream top item",
                )
            {
                tracing::log::warn!(target: PROJECT_ID, "Duplicate ID: {}", err);
                Ok(())
            } else {
                Err(err)
            }
        }
    }
}
