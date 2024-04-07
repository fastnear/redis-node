mod common;
mod redis_db;

use dotenv::dotenv;
use redis::aio::MultiplexedConnection;
use redis_db::RedisDB;
use std::{env, fs};

pub type BlockHeight = u64;

const PROJECT_ID: &str = "saver";
const SAFE_OFFSET: u64 = 100;

const FINAL_BLOCKS_KEY: &str = "final_blocks";
const BLOCK_KEY: &str = "block";
const CACHE_EXPIRATION: std::time::Duration = std::time::Duration::from_secs(60);

fn read_folder(path: &str) -> Vec<String> {
    let mut entries: Vec<String> = fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
        .collect();
    entries.sort();
    entries
}

fn last_block_height(path: &str) -> Option<BlockHeight> {
    let last_outer = read_folder(path).last().cloned()?;
    let last_thousand = read_folder(&format!("{}/{}", path, last_outer))
        .last()
        .cloned()?;
    let last_file = read_folder(&format!("{}/{}/{}", path, last_outer, last_thousand))
        .last()
        .cloned()?;
    last_file.split_once(".").unwrap().0.parse().ok()
}

#[tokio::main]
async fn main() {
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    let save_every_n = env::var("SAVE_EVERY_N")
        .expect("SAVE_EVERY_N is not set")
        .parse()
        .unwrap();

    let chain_id = env::var("CHAIN_ID").expect("CHAIN_ID is not set");
    common::setup_tracing("redis=info,saver=info");
    let mut read_db = RedisDB::new(None).await;

    let path = env::var("ARCHIVAL_DATA_PATH").expect("ARCHIVAL_DATA_PATH is not set");
    let last_block_height = last_block_height(&path);

    let (id, _key_values) = read_db
        .xread(1, FINAL_BLOCKS_KEY, "0")
        .await
        .expect("Failed to get the first block from Redis")
        .into_iter()
        .next()
        .unwrap();
    let first_block_height: BlockHeight = id.split_once("-").unwrap().0.parse().unwrap();
    let min_start_block =
        (first_block_height + SAFE_OFFSET + save_every_n) / save_every_n * save_every_n;

    let mut start_block = last_block_height
        .unwrap_or(min_start_block)
        .saturating_sub(1)
        / save_every_n
        * save_every_n;

    if start_block < first_block_height + SAFE_OFFSET {
        tracing::warn!(target: PROJECT_ID, "Start block is too early, resetting to {}", min_start_block);
        start_block = min_start_block;
    }

    let mut last_id = format!("{}-0", start_block);
    tracing::info!(target: PROJECT_ID, "Resuming from {}", last_id);

    let mut cache_db = RedisDB::new(Some(
        env::var("CACHE_REDIS_URL").expect("Missing CACHE_REDIS_URL env var"),
    ))
    .await;

    let mut blocks = vec![];
    loop {
        let res = read_db.xread(1, FINAL_BLOCKS_KEY, &last_id).await;
        let res = match res {
            Ok(res) => res,
            Err(err) => {
                tracing::error!(target: PROJECT_ID, "Error: {}", err);
                tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
                let _ = read_db.reconnect().await;
                continue;
            }
        };
        let (id, key_values) = res.into_iter().next().unwrap();
        assert_eq!(key_values.len(), 1, "Expected 1 key-value pair");
        let (key, value) = key_values.into_iter().next().unwrap();
        assert_eq!(key, BLOCK_KEY, "Expected key to be block");
        let block_height: BlockHeight = id.split_once("-").unwrap().0.parse().unwrap();
        tracing::info!(target: PROJECT_ID, "Processing block: {}", block_height);
        if blocks.get(0).map(|(height, _block)| *height).unwrap_or(0) / save_every_n
            != block_height / save_every_n
        {
            save_blocks(&blocks, path.as_str(), save_every_n).unwrap();
            blocks.clear();
        }
        // Update cache

        with_retries!(cache_db, |connection| async {
            set_block(connection, &chain_id, block_height, &value).await
        })
        .expect("Failed to set block in cache");

        with_retries!(cache_db, |connection| async {
            set_last_block_height(connection, &chain_id, block_height).await
        })
        .expect("Failed to set last block height in cache");

        blocks.push((block_height, value));
        last_id = id;
    }
}

fn save_blocks(
    blocks: &[(BlockHeight, String)],
    path: &str,
    save_every_n: u64,
) -> std::io::Result<()> {
    if blocks.is_empty() {
        return Ok(());
    }
    let starting_block = blocks[0].0 / save_every_n * save_every_n;
    let padded_block_height = format!("{:0>12}", starting_block);
    let path = format!(
        "{}/{}/{}",
        path,
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

pub(crate) async fn set_block(
    connection: &mut MultiplexedConnection,
    chain_id: &str,
    block_height: BlockHeight,
    block: &str,
) -> Result<(), redis::RedisError> {
    let key = format!("b:{}:{}", chain_id, block_height);
    redis::cmd("SET")
        .arg(&key)
        .arg(block)
        .arg("EX")
        .arg(CACHE_EXPIRATION.as_secs())
        .query_async(connection)
        .await
}

pub(crate) async fn set_last_block_height(
    connection: &mut MultiplexedConnection,
    chain_id: &str,
    block_height: BlockHeight,
) -> Result<(), redis::RedisError> {
    let key = format!("meta:{}:last_block", chain_id);
    redis::cmd("SET")
        .arg(&key)
        .arg(block_height)
        .query_async(connection)
        .await
}
