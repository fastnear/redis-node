mod redis_db;

use dotenv::dotenv;
use near_indexer::near_primitives::types::BlockHeight;
use redis_db::RedisDB;
use std::io::Write;
use std::{env, fs};

const FINAL_BLOCKS_KEY: &str = "final_blocks";
const BLOCK_KEY: &str = "block";

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

    let path = env::var("DATA_PATH").expect("DATA_PATH is not set");
    // Get env var for Redis URL if set
    let redis_url_opt: Option<String> = env::var("CACHE_REDIS_URL").ok();
    let last_block_height = last_block_height(&path);
    let mut last_id = last_block_height
        .map(|h| format!("{}-0", h))
        .unwrap_or("0".to_string());
    println!("Resuming from {}", last_block_height.unwrap_or(0));

    // Uses CACHE_REDIS_URL env var if it's set, otherwise provides None
    let mut db = RedisDB::new(redis_url_opt).await;

    loop {
        let res = db.xread(1, FINAL_BLOCKS_KEY, &last_id).await;
        let res = match res {
            Ok(res) => res,
            Err(err) => {
                eprintln!("Error: {}", err);
                tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
                let _ = db.reconnect().await;
                continue;
            }
        };
        let (id, key_values) = res.into_iter().next().unwrap();
        assert_eq!(key_values.len(), 1, "Expected 1 key-value pair");
        let (key, value) = key_values.into_iter().next().unwrap();
        assert_eq!(key, BLOCK_KEY, "Expected key to be block");
        let block_height: BlockHeight = id.split_once("-").unwrap().0.parse().unwrap();
        let padded_block_height = format!("{:0>12}", block_height);
        let block_dir = format!(
            "{}/{}/{}",
            path,
            &padded_block_height[..6],
            &padded_block_height[6..9]
        );
        let block_file = format!("{}/{}.json", block_dir, padded_block_height);
        println!("Saving {} bytes to file {}", value.len(), block_file);
        fs::create_dir_all(&block_dir).unwrap();
        let mut file = fs::File::create(&block_file).unwrap();
        file.write_all(value.as_bytes()).unwrap();
        last_id = id;
    }
}
