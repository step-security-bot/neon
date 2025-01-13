use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use postgres_client::tls::MakeTlsConnect;
use postgres_client::CancelToken;
use pq_proto::CancelKeyData;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::{debug, info};

use crate::auth::backend::{BackendIpAllowlist, ComputeUserInfo};
use crate::auth::{check_peer_addr_is_in_list, AuthError};
use crate::config::ComputeConfig;
use crate::context::RequestContext;
use crate::error::ReportableError;
use crate::ext::LockExt;
use crate::metrics::{CancellationRequest, Metrics};
use crate::rate_limiter::LeakyBucketRateLimiter;
use crate::redis::keys::KeyPrefix;
use crate::redis::kv_ops::{RedisKVClient, RedisKVClientExt};
use crate::tls::postgres_rustls::MakeRustlsConnect;

pub type CancellationHandlerMain = CancellationHandler<RedisKVClient>;
pub(crate) type CancellationHandlerMainInternal = RedisKVClient;

type IpSubnetKey = IpNet;

/// Enables serving `CancelRequest`s.
///
/// If `CancellationPublisher` is available, cancel request will be used to publish the cancellation key to other proxy instances.
pub struct CancellationHandler<P: RedisKVClientExt> {
    compute_config: &'static ComputeConfig,
    client: Option<Arc<Mutex<P>>>,
    // rate limiter of cancellation requests
    limiter: Arc<std::sync::Mutex<LeakyBucketRateLimiter<IpSubnetKey>>>,
}

#[derive(Debug, Error)]
pub(crate) enum CancelError {
    #[error("{0}")]
    IO(#[from] std::io::Error),

    #[error("{0}")]
    Postgres(#[from] postgres_client::Error),

    #[error("rate limit exceeded")]
    RateLimit,

    #[error("IP is not allowed")]
    IpNotAllowed,

    #[error("Authentication backend error")]
    AuthError(#[from] AuthError),

    #[error("key not found")]
    NotFound,

    #[error("proxy service error")]
    InternalError,
}

impl ReportableError for CancelError {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        match self {
            CancelError::IO(_) => crate::error::ErrorKind::Compute,
            CancelError::Postgres(e) if e.as_db_error().is_some() => {
                crate::error::ErrorKind::Postgres
            }
            CancelError::Postgres(_) => crate::error::ErrorKind::Compute,
            CancelError::RateLimit => crate::error::ErrorKind::RateLimit,
            CancelError::IpNotAllowed => crate::error::ErrorKind::User,
            CancelError::NotFound => crate::error::ErrorKind::User,
            CancelError::AuthError(_) => crate::error::ErrorKind::ControlPlane,
            CancelError::InternalError => crate::error::ErrorKind::Service,
        }
    }
}

impl<P: RedisKVClientExt> CancellationHandler<P> {
    pub(crate) fn get_key(self: &Arc<Self>) -> Session<P> {
        // we intentionally generate a random "backend pid" and "secret key" here.
        // we use the corresponding u64 as an identifier for the
        // actual endpoint+pid+secret for postgres/pgbouncer.
        //
        // if we forwarded the backend_pid from postgres to the client, there would be a lot
        // of overlap between our computes as most pids are small (~100).
        // let node_id_bits: u64 = (node_id as u64) << 32;

        let key: CancelKeyData = rand::random();

        debug!("registered new query cancellation key {key}");
        Session {
            key,
            cancellation_handler: Arc::clone(self),
        }
    }

    // write the key to the redis
    pub(crate) async fn write_cancel_key(
        &self,
        key: &CancelKeyData,
        cancel_closure: CancelClosure,
    ) -> Result<(), CancelError> {
        let prefix_key: KeyPrefix = KeyPrefix::Cancel(*key);

        let redis_key = prefix_key.build_redis_key().map_err(|e| {
            tracing::warn!("failed to build redis key: {e}");
            CancelError::InternalError
        })?;

        let closure_json = serde_json::to_string(&cancel_closure).map_err(|e| {
            tracing::warn!("failed to serialize cancel closure: {e}");
            CancelError::InternalError
        })?;

        if let Some(client) = &self.client {
            client
                .lock()
                .await
                .hset(redis_key, "data", closure_json)
                .await
                .map_err(|e| {
                    tracing::warn!("failed to set cancel key in redis: {e}");
                    CancelError::InternalError
                })?;
        }

        Ok(())
    }

    /// Try to cancel a running query for the corresponding connection.
    /// If the cancellation key is not found, it will be published to Redis.
    /// check_allowed - if true, check if the IP is allowed to cancel the query.
    /// Will fetch IP allowlist internally.
    ///
    /// return Result primarily for tests
    pub(crate) async fn cancel_session<T: BackendIpAllowlist>(
        &self,
        key: CancelKeyData,
        ctx: RequestContext,
        check_allowed: bool,
        auth_backend: &T,
    ) -> Result<(), CancelError> {
        let subnet_key = match ctx.peer_addr() {
            IpAddr::V4(ip) => IpNet::V4(Ipv4Net::new_assert(ip, 24).trunc()), // use defaut mask here
            IpAddr::V6(ip) => IpNet::V6(Ipv6Net::new_assert(ip, 64).trunc()),
        };
        if !self.limiter.lock_propagate_poison().check(subnet_key, 1) {
            // log only the subnet part of the IP address to know which subnet is rate limited
            tracing::warn!("Rate limit exceeded. Skipping cancellation message, {subnet_key}");
            Metrics::get()
                .proxy
                .cancellation_requests_total
                .inc(CancellationRequest {
                    kind: crate::metrics::CancellationOutcome::RateLimitExceeded,
                });
            return Err(CancelError::RateLimit);
        }

        let prefix_key: KeyPrefix = KeyPrefix::Cancel(key);

        let redis_key = prefix_key.build_redis_key().map_err(|e| {
            tracing::warn!("failed to build redis key: {e}");
            CancelError::InternalError
        })?;

        // NB: we should immediately release the lock after cloning the token.
        let cancel_state_str = match self
            .client
            .as_ref()
            .expect("Client is not set")
            .lock()
            .await
            .hget_all::<String, String>(redis_key)
            .await
        {
            Ok(state) => Some(state),
            Err(e) => {
                tracing::warn!("failed to get cancel state from redis: {e}");
                return Err(CancelError::InternalError);
            }
        };

        let cancel_state: Option<CancelClosure> = match cancel_state_str {
            Some(state) => {
                let cancel_closure: CancelClosure = serde_json::from_str(&state).map_err(|e| {
                    tracing::warn!("failed to deserialize cancel state: {e}");
                    CancelError::InternalError
                })?;
                Some(cancel_closure)
            }
            None => None,
        };

        let Some(cancel_closure) = cancel_state else {
            tracing::warn!("query cancellation key not found: {key}");
            Metrics::get()
                .proxy
                .cancellation_requests_total
                .inc(CancellationRequest {
                    kind: crate::metrics::CancellationOutcome::NotFound,
                });
            return Err(CancelError::NotFound);
        };

        if check_allowed {
            let ip_allowlist = auth_backend
                .get_allowed_ips(&ctx, &cancel_closure.user_info)
                .await
                .map_err(CancelError::AuthError)?;

            if !check_peer_addr_is_in_list(&ctx.peer_addr(), &ip_allowlist) {
                // log it here since cancel_session could be spawned in a task
                tracing::warn!(
                    "IP is not allowed to cancel the query: {key}, address: {}",
                    ctx.peer_addr()
                );
                return Err(CancelError::IpNotAllowed);
            }
        }

        Metrics::get()
            .proxy
            .cancellation_requests_total
            .inc(CancellationRequest {
                kind: crate::metrics::CancellationOutcome::Found,
            });
        info!("cancelling query per user's request using key {key}");
        cancel_closure.try_cancel_query(self.compute_config).await
    }
}

impl CancellationHandler<RedisKVClient> {
    pub fn new(
        compute_config: &'static ComputeConfig,
        client: Option<Arc<Mutex<RedisKVClient>>>,
    ) -> Self {
        Self {
            compute_config,
            client,
            limiter: Arc::new(std::sync::Mutex::new(
                LeakyBucketRateLimiter::<IpSubnetKey>::new_with_shards(
                    LeakyBucketRateLimiter::<IpSubnetKey>::DEFAULT,
                    64,
                ),
            )),
        }
    }
}

/// This should've been a [`std::future::Future`], but
/// it's impossible to name a type of an unboxed future
/// (we'd need something like `#![feature(type_alias_impl_trait)]`).
#[derive(Clone, Serialize, Deserialize)]
pub struct CancelClosure {
    socket_addr: SocketAddr,
    cancel_token: CancelToken,
    hostname: String, // for pg_sni router
    user_info: ComputeUserInfo,
}

impl CancelClosure {
    pub(crate) fn new(
        socket_addr: SocketAddr,
        cancel_token: CancelToken,
        hostname: String,
        user_info: ComputeUserInfo,
    ) -> Self {
        Self {
            socket_addr,
            cancel_token,
            hostname,
            user_info,
        }
    }
    /// Cancels the query running on user's compute node.
    pub(crate) async fn try_cancel_query(
        self,
        compute_config: &ComputeConfig,
    ) -> Result<(), CancelError> {
        let socket = TcpStream::connect(self.socket_addr).await?;

        let mut mk_tls =
            crate::tls::postgres_rustls::MakeRustlsConnect::new(compute_config.tls.clone());
        let tls = <MakeRustlsConnect as MakeTlsConnect<tokio::net::TcpStream>>::make_tls_connect(
            &mut mk_tls,
            &self.hostname,
        )
        .map_err(|e| {
            CancelError::IO(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))
        })?;

        self.cancel_token.cancel_query_raw(socket, tls).await?;
        debug!("query was cancelled");
        Ok(())
    }
}

/// Helper for registering query cancellation tokens.
pub(crate) struct Session<P>
where
    P: RedisKVClientExt + Send + Sync + 'static,
{
    /// The user-facing key identifying this session.
    key: CancelKeyData,
    cancellation_handler: Arc<CancellationHandler<P>>,
}

impl<P> Session<P>
where
    P: RedisKVClientExt + Send + Sync + 'static,
{
    pub(crate) fn key(&self) -> &CancelKeyData {
        &self.key
    }

    pub(crate) async fn remove_cancel_key(&self) -> Result<(), CancelError> {
        let client = match &self.cancellation_handler.client {
            Some(client) => Arc::clone(client),
            None => return Ok(()),
        };

        let key = self.key;
        let prefix_key: KeyPrefix = KeyPrefix::Cancel(key);

        let redis_key: String = match prefix_key.build_redis_key() {
            Ok(key) => key,
            Err(e) => {
                tracing::warn!("failed to build redis key: {e}");
                return Err(CancelError::InternalError);
            }
        };

        client
            .lock()
            .await
            .hdel(redis_key, "data".to_string())
            .await
            .map_err(|e| {
                tracing::warn!("failed to delete cancel key from redis: {e}");
                CancelError::InternalError
            })?;

        Ok(())
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::config::RetryConfig;
    use crate::tls::client_config::compute_client_config_with_certs;

    fn config() -> ComputeConfig {
        let retry = RetryConfig {
            base_delay: Duration::from_secs(1),
            max_retries: 5,
            backoff_factor: 2.0,
        };

        ComputeConfig {
            retry,
            tls: Arc::new(compute_client_config_with_certs(std::iter::empty())),
            timeout: Duration::from_secs(2),
        }
    }

    // #[tokio::test]
    // async fn cancel_session_noop_regression() {
    //     let handler = Arc::new(CancellationHandler::<RedisKVClient>::new(
    //         Box::leak(Box::new(config())),
    //         Arc::new(DashSet::default()),
    //         None,
    //     ));

    //     handler
    //         .cancel_session(
    //             CancelKeyData {
    //                 backend_pid: 0,
    //                 cancel_key: 0,
    //             },
    //             Uuid::new_v4(),
    //             "127.0.0.1".parse().unwrap(),
    //             true,
    //         )
    //         .await
    //         .unwrap();
    // }
}
