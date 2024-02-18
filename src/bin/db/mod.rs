use redis::aio::Connection;
use redis::Client;
use std::env;

pub struct DB {
    pub client: Client,
    pub connection: Connection,
}

impl DB {
    pub async fn new() -> Self {
        let client = Client::open(env::var("REDIS_URL").expect("Missing REDIS_URL env var"))
            .expect("Failed to connect to Redis");
        let connection = client
            .get_async_connection()
            .await
            .expect("Failed to on Redis connection");
        Self { client, connection }
    }
}

impl DB {
    pub async fn set(&mut self, key: &str, value: &str) -> redis::RedisResult<String> {
        redis::cmd("SET")
            .arg(key)
            .arg(value)
            .query_async(&mut self.connection)
            .await
    }

    pub async fn get(&mut self, key: &str) -> redis::RedisResult<String> {
        redis::cmd("GET")
            .arg(key)
            .query_async(&mut self.connection)
            .await
    }

    pub async fn xadd(
        &mut self,
        key: &str,
        id: &str,
        data: &[(String, String)],
    ) -> redis::RedisResult<String> {
        redis::cmd("XADD")
            .arg(key)
            .arg(id)
            .arg(data)
            .query_async(&mut self.connection)
            .await
    }
}
