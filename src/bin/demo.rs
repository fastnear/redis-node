use dotenv::dotenv;

mod db;

const MAX_LEN: usize = 100;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    let args: Vec<String> = std::env::args().collect();
    let command = args.get(1).map(|arg| arg.as_str()).unwrap_or("");

    let mut db = db::DB::new().await;
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
                let res = db.xadd(stream_key, &id, &data, Some(MAX_LEN)).await;
                match res {
                    Ok(res) => {
                        println!("Added {} with {}", res, timestamp_ms);
                    }
                    Err(res) => {
                        if res.kind() == redis::ErrorKind::ResponseError {
                            println!("Duplicate ID {}", id);
                        } else {
                            return Err(res.into());
                        }
                    }
                };
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }
        "consume" => {
            let mut last_id = "0".to_string();
            let mut cnt = 0;
            loop {
                let res = db.xread(1, stream_key, &last_id).await?;
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
