use dotenv::dotenv;

mod db;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    let mut db = db::DB::new().await;
    // let key = "test-yo";
    // let _ = db.set(key, "bar").await?;
    // let value = db.get(key).await?;
    // println!("res: {}", value);

    let streamkey = "test-stream";
    // Stream current timestamp every 1 second
    loop {
        let now = std::time::SystemTime::now();
        let since_the_epoch = now.duration_since(std::time::UNIX_EPOCH)?;
        let timestamp = since_the_epoch.as_secs();
        let id = format!("{}-*", timestamp);
        let data = vec![
            ("date".to_string(), timestamp.to_string()),
            ("yo".to_string(), "bar".to_string()),
        ];
        let _ = db.xadd(streamkey, &id, &data).await?;
        println!("Inserted timestamp: {}", timestamp);
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
    }

    Ok(())
}
