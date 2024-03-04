use dotenv::dotenv;

mod redis_db;

const MAX_LEN: usize = 100;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    let args: Vec<String> = std::env::args().collect();
    let command = args.get(1).map(|arg| arg.as_str()).unwrap_or("");

    let mut db = redis_db::RedisDB::new().await;
    let stream_key = "test-stream";

    match command {
        "set-get" => {
            let key = "test-yo";
            let _ = db.set(key, "bar").await?;
            let value = db.get(key).await?;
            println!("res: {}", value);
        }
        "produce" => {
            // Stream current timestamp every 1 second
            loop {
                let now = std::time::SystemTime::now();
                let since_the_epoch = now.duration_since(std::time::UNIX_EPOCH)?;
                let timestamp_ms = since_the_epoch.as_millis();
                let timestamp = timestamp_ms / 1000;
                // Note, ID `{}-0` should be used to avoid duplicates, `{}-*` to add all
                let id = format!("{}-0", timestamp);
                let data = vec![
                    ("date".to_string(), timestamp_ms.to_string()),
                    ("yo".to_string(), "bar".to_string()),
                ];
                let mut delay = tokio::time::Duration::from_millis(100);
                for _ in 0..10 {
                    let res = db.xadd(stream_key, &id, &data, Some(MAX_LEN)).await;
                    match res {
                        Ok(res) => {
                            println!("Added {} with {}", res, timestamp_ms);
                        }
                        Err(err) => {
                            if err.kind() == redis::ErrorKind::ResponseError {
                                println!("Duplicate ID {}", id);
                            } else {
                                eprintln!("Error: {}", err);
                                tokio::time::sleep(delay).await;
                                let _ = db.reconnect().await;
                                delay *= 2;
                                continue;
                            }
                        }
                    }
                    break;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }
        "consume" => {
            let last_id = db.last_id(stream_key).await?;
            if let Some(last_id) = last_id.as_ref() {
                println!("Last ID: {}", last_id);
            }
            let mut last_id = last_id.unwrap_or("0".to_string());
            let mut cnt = 0;
            loop {
                let res = db.xread(1, stream_key, &last_id).await;
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
                println!("#{} {}: {:?}", cnt, id, key_values);
                last_id = id;
                cnt += 1;
            }
        }
        _ => panic!("You have to pass `set-get`, `produce` or `consume` arg"),
    }

    Ok(())
}
