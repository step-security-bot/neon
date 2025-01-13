use async_trait::async_trait;
use redis::{AsyncCommands, ToRedisArgs};

use super::connection_with_credentials_provider::ConnectionWithCredentialsProvider;
use crate::rate_limiter::{GlobalRateLimiter, RateBucketInfo};

pub struct RedisKVClient {
    client: ConnectionWithCredentialsProvider,
    limiter: GlobalRateLimiter,
}

#[async_trait]
pub trait RedisKVClientExt: Sized + Send + Sync + 'static {
    fn new(
        client: ConnectionWithCredentialsProvider,
        info: &'static [RateBucketInfo],
    ) -> anyhow::Result<Self>;

    async fn try_connect(&mut self) -> anyhow::Result<()>;

    async fn hset<K, F, V>(&mut self, key: K, field: F, value: V) -> anyhow::Result<()>
    where
        K: ToRedisArgs + Send + Sync,
        F: ToRedisArgs + Send + Sync,
        V: ToRedisArgs + Send + Sync;

    async fn hset_multiple<K, V>(&mut self, key: &str, items: &[(K, V)]) -> anyhow::Result<()>
    where
        K: ToRedisArgs + Send + Sync,
        V: ToRedisArgs + Send + Sync;

    async fn hget<K, F, V>(&mut self, key: K, field: F) -> anyhow::Result<V>
    where
        K: ToRedisArgs + Send + Sync,
        F: ToRedisArgs + Send + Sync,
        V: redis::FromRedisValue;

    async fn hget_all<K, V>(&mut self, key: K) -> anyhow::Result<V>
    where
        K: ToRedisArgs + Send + Sync,
        V: redis::FromRedisValue;

    async fn hdel<K, F>(&mut self, key: K, field: F) -> anyhow::Result<()>
    where
        K: ToRedisArgs + Send + Sync,
        F: ToRedisArgs + Send + Sync;
}

#[async_trait]
impl RedisKVClientExt for RedisKVClient {
    fn new(
        client: ConnectionWithCredentialsProvider,
        info: &'static [RateBucketInfo],
    ) -> anyhow::Result<Self> {
        Ok(Self {
            client,
            // region_id,
            limiter: GlobalRateLimiter::new(info.into()),
        })
    }

    async fn try_connect(&mut self) -> anyhow::Result<()> {
        match self.client.connect().await {
            Ok(()) => {}
            Err(e) => {
                tracing::error!("failed to connect to redis: {e}");
                return Err(e);
            }
        }
        Ok(())
    }

    async fn hset<K, F, V>(&mut self, key: K, field: F, value: V) -> anyhow::Result<()>
    where
        K: ToRedisArgs + Send + Sync,
        F: ToRedisArgs + Send + Sync,
        V: ToRedisArgs + Send + Sync,
    {
        if !self.limiter.check() {
            tracing::info!("Rate limit exceeded. Skipping hset");
            return Err(anyhow::anyhow!("Rate limit exceeded"));
        }

        match self.client.hset(key, field, value).await {
            Ok(()) => Ok(()),
            Err(e) => {
                tracing::error!("failed to set a key-value pair: {e}");
                Err(e.into())
            }
        }
    }

    async fn hset_multiple<K, V>(&mut self, key: &str, items: &[(K, V)]) -> anyhow::Result<()>
    where
        K: ToRedisArgs + Send + Sync,
        V: ToRedisArgs + Send + Sync,
    {
        if !self.limiter.check() {
            tracing::info!("Rate limit exceeded. Skipping hset_multiple");
            return Err(anyhow::anyhow!("Rate limit exceeded"));
        }

        match self.client.hset_multiple(key, items).await {
            Ok(()) => Ok(()),
            Err(e) => {
                tracing::error!("failed to set a key-value pair: {e}");
                Err(e.into())
            }
        }
    }

    async fn hget<K, F, V>(&mut self, key: K, field: F) -> anyhow::Result<V>
    where
        K: ToRedisArgs + Send + Sync,
        F: ToRedisArgs + Send + Sync,
        V: redis::FromRedisValue,
    {
        if !self.limiter.check() {
            tracing::info!("Rate limit exceeded. Skipping hget");
            return Err(anyhow::anyhow!("Rate limit exceeded"));
        }

        match self.client.hget(key, field).await {
            Ok(value) => Ok(value),
            Err(e) => {
                tracing::error!("failed to get a value: {e}");
                Err(e.into())
            }
        }
    }

    async fn hget_all<K, V>(&mut self, key: K) -> anyhow::Result<V>
    where
        K: ToRedisArgs + Send + Sync,
        V: redis::FromRedisValue,
    {
        if !self.limiter.check() {
            tracing::info!("Rate limit exceeded. Skipping hgetall");
            return Err(anyhow::anyhow!("Rate limit exceeded"));
        }

        match self.client.hgetall(key).await {
            Ok(value) => Ok(value),
            Err(e) => {
                tracing::error!("failed to get a value: {e}");
                Err(e.into())
            }
        }
    }

    async fn hdel<K, F>(&mut self, key: K, field: F) -> anyhow::Result<()>
    where
        K: ToRedisArgs + Send + Sync,
        F: ToRedisArgs + Send + Sync,
    {
        if !self.limiter.check() {
            tracing::info!("Rate limit exceeded. Skipping hdel");
            return Err(anyhow::anyhow!("Rate limit exceeded"));
        }

        match self.client.hdel(key, field).await {
            Ok(()) => Ok(()),
            Err(e) => {
                tracing::error!("failed to delete a key-value pair: {e}");
                Err(e.into())
            }
        }
    }
}
