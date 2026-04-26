//! Per-IP rate-limiting middleware for Rust Lambda HTTP handlers.
//!
//! A fixed-window counter backed by DynamoDB. The primary key for each
//! counter row is `"{ip}#{window_bucket}"` where `window_bucket = now / window_secs`.
//! Rows carry a TTL so DynamoDB cleans them up for us.
//!
//! Response headers follow the IETF draft (unprefixed):
//!
//! * `RateLimit-Limit`
//! * `RateLimit-Remaining`
//! * `RateLimit-Reset`
//!
//! On breach we return a plain JSON `429` with `Retry-After`.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use aws_sdk_dynamodb::types::AttributeValue;
use http::{HeaderValue, Response};
use lambda_http::{tracing, Body};
use serde::Serialize;
use tower::{Layer, Service};

use crate::ip_extractor::extract_ip;

/// Static, cheap-to-clone configuration for [`RateLimitLayer`].
#[derive(Clone)]
pub struct RateLimitConfig {
    pub table_name: String,
    pub max_requests: u32,
    pub window_secs: u64,
}

/// Tower [`Layer`] that enforces a per-IP fixed-window rate limit.
#[derive(Clone)]
pub struct RateLimitLayer {
    config: Arc<RateLimitConfig>,
    client: aws_sdk_dynamodb::Client,
}

impl RateLimitLayer {
    pub fn new(config: RateLimitConfig, client: aws_sdk_dynamodb::Client) -> Self {
        Self {
            config: Arc::new(config),
            client,
        }
    }
}

impl<S> Layer<S> for RateLimitLayer {
    type Service = RateLimitService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        let store: Arc<dyn RateLimitStore> = Arc::new(DynamoDbRateLimitStore {
            client: self.client.clone(),
        });
        RateLimitService {
            inner,
            store,
            config: Arc::clone(&self.config),
        }
    }
}

/// Tower [`Service`] that wraps an inner service, checks the counter on every
/// request, and either short-circuits with a 429 or forwards to the inner
/// service and stamps the response with `RateLimit-*` headers.
pub struct RateLimitService<S> {
    inner: S,
    store: Arc<dyn RateLimitStore>,
    config: Arc<RateLimitConfig>,
}

impl<S> Service<http::Request<Body>> for RateLimitService<S>
where
    S: Service<http::Request<Body>, Response = Response<Body>> + Send + 'static,
    S::Future: Send,
    S::Error: Send,
{
    type Response = Response<Body>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let config = Arc::clone(&self.config);
        let store = Arc::clone(&self.store);

        // Extract everything we need from the request up front, because
        // `inner.call(request)` consumes it by move.
        let ip = extract_ip(&request).map(|ip| ip.to_string());

        let inner_future = self.inner.call(request);

        Box::pin(async move {
            let Some(ip) = ip else {
                // No identifiable client: fail open so we don't lock out
                // legitimate traffic when a misbehaving proxy drops headers.
                tracing::warn!("rate_limit: could not determine client IP, allowing request");
                return inner_future.await;
            };

            let now = current_epoch_secs();
            let window = config.window_secs.max(1);
            let bucket = now / window;
            let reset_at = bucket.saturating_add(1).saturating_mul(window);
            let seconds_until_reset = reset_at.saturating_sub(now);

            let pk = format!("{ip}#{bucket}");
            // Keep the row for one extra window so late-arriving requests in
            // the same bucket still observe a consistent counter.
            let ttl = reset_at.saturating_add(window);

            let count = match store.increment_and_get(&config.table_name, &pk, ttl).await {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!(error = %e, "rate_limit: DynamoDB error");
                    return Ok(build_500_response());
                }
            };

            let limit = config.max_requests;
            if count > limit {
                return Ok(build_429_response(limit, reset_at, seconds_until_reset));
            }

            let mut response = inner_future.await?;
            let remaining = limit.saturating_sub(count);
            append_rate_limit_headers(response.headers_mut(), limit, remaining, reset_at);
            Ok(response)
        })
    }
}

fn current_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn append_rate_limit_headers(
    headers: &mut http::HeaderMap,
    limit: u32,
    remaining: u32,
    reset_at: u64,
) {
    if let Ok(v) = HeaderValue::from_str(&limit.to_string()) {
        headers.insert("RateLimit-Limit", v);
    }
    if let Ok(v) = HeaderValue::from_str(&remaining.to_string()) {
        headers.insert("RateLimit-Remaining", v);
    }
    if let Ok(v) = HeaderValue::from_str(&reset_at.to_string()) {
        headers.insert("RateLimit-Reset", v);
    }
}

#[derive(Serialize)]
struct RateLimitErrorBody<'a> {
    error: &'a str,
    retry_after: u64,
}

fn build_429_response(limit: u32, reset_at: u64, seconds_until_reset: u64) -> Response<Body> {
    let body = serde_json::to_string(&RateLimitErrorBody {
        error: "rate limit exceeded",
        retry_after: seconds_until_reset,
    })
    .unwrap_or_else(|_| r#"{"error":"rate limit exceeded"}"#.to_string());

    Response::builder()
        .status(429)
        .header("content-type", "application/json")
        .header("cache-control", "no-store")
        .header("retry-after", seconds_until_reset.to_string())
        .header("RateLimit-Limit", limit.to_string())
        .header("RateLimit-Remaining", "0")
        .header("RateLimit-Reset", reset_at.to_string())
        .body(body.into())
        .expect("valid 429 response")
}

fn build_500_response() -> Response<Body> {
    Response::builder()
        .status(500)
        .header("content-type", "application/json")
        .header("cache-control", "no-store")
        .body(r#"{"error":"internal server error"}"#.into())
        .expect("valid 500 response")
}

// ---------------------------------------------------------------------------
// Store abstraction. A trait here makes the service trivially unit-testable
// with an in-memory mock, without needing a local DynamoDB.
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
trait RateLimitStore: Send + Sync {
    async fn increment_and_get(
        &self,
        table_name: &str,
        pk: &str,
        ttl: u64,
    ) -> Result<u32, RateLimitStoreError>;
}

#[derive(Debug, thiserror::Error)]
enum RateLimitStoreError {
    #[error("DynamoDB error: {0}")]
    DynamoDb(String),
}

struct DynamoDbRateLimitStore {
    client: aws_sdk_dynamodb::Client,
}

#[async_trait::async_trait]
impl RateLimitStore for DynamoDbRateLimitStore {
    async fn increment_and_get(
        &self,
        table_name: &str,
        pk: &str,
        ttl: u64,
    ) -> Result<u32, RateLimitStoreError> {
        let result = self
            .client
            .update_item()
            .table_name(table_name)
            .key("pk", AttributeValue::S(pk.to_string()))
            .update_expression("ADD #calls :one SET #ttl = if_not_exists(#ttl, :ttl_val)")
            .expression_attribute_names("#calls", "calls")
            .expression_attribute_names("#ttl", "ttl")
            .expression_attribute_values(":one", AttributeValue::N("1".to_string()))
            .expression_attribute_values(":ttl_val", AttributeValue::N(ttl.to_string()))
            .return_values(aws_sdk_dynamodb::types::ReturnValue::UpdatedNew)
            .send()
            .await
            .map_err(|e| RateLimitStoreError::DynamoDb(e.to_string()))?;

        let count = result
            .attributes()
            .and_then(|attrs| attrs.get("calls"))
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<u32>().ok())
            .unwrap_or(1);

        Ok(count)
    }
}

#[cfg(test)]
impl<S> RateLimitService<S> {
    fn with_store(inner: S, store: Arc<dyn RateLimitStore>, config: Arc<RateLimitConfig>) -> Self {
        Self {
            inner,
            store,
            config,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{Request, StatusCode};
    use std::convert::Infallible;
    use std::sync::Mutex;
    use tower::ServiceExt;

    struct MockRateLimitStore {
        counters: Mutex<std::collections::HashMap<String, u32>>,
        fail: bool,
    }

    impl MockRateLimitStore {
        fn new() -> Self {
            Self {
                counters: Mutex::new(std::collections::HashMap::new()),
                fail: false,
            }
        }

        fn failing() -> Self {
            Self {
                counters: Mutex::new(std::collections::HashMap::new()),
                fail: true,
            }
        }
    }

    #[async_trait::async_trait]
    impl RateLimitStore for MockRateLimitStore {
        async fn increment_and_get(
            &self,
            _table_name: &str,
            pk: &str,
            _ttl: u64,
        ) -> Result<u32, RateLimitStoreError> {
            if self.fail {
                return Err(RateLimitStoreError::DynamoDb("mock failure".to_string()));
            }
            let mut counters = self.counters.lock().unwrap();
            let count = counters.entry(pk.to_string()).or_insert(0);
            *count += 1;
            Ok(*count)
        }
    }

    fn test_config(max: u32) -> Arc<RateLimitConfig> {
        Arc::new(RateLimitConfig {
            table_name: "test-table".to_string(),
            max_requests: max,
            window_secs: 60,
        })
    }

    fn request_with_ip(ip: &str) -> Request<Body> {
        Request::builder()
            .method("GET")
            .uri("http://example.com/test")
            .header("x-forwarded-for", ip)
            .body(Body::Empty)
            .unwrap()
    }

    async fn ok_handler(_req: Request<Body>) -> Result<Response<Body>, Infallible> {
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from(r#"{"status":"ok"}"#))
            .unwrap())
    }

    #[tokio::test]
    async fn under_limit_calls_inner_service() {
        let config = test_config(10);
        let store: Arc<dyn RateLimitStore> = Arc::new(MockRateLimitStore::new());
        let service = RateLimitService::with_store(tower::service_fn(ok_handler), store, config);

        let response = service
            .oneshot(request_with_ip("203.0.113.1"))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("RateLimit-Limit").unwrap(), "10");
        assert_eq!(response.headers().get("RateLimit-Remaining").unwrap(), "9");
        assert!(response.headers().contains_key("RateLimit-Reset"));
    }

    #[tokio::test]
    async fn over_limit_returns_429() {
        let config = test_config(1);
        let store: Arc<dyn RateLimitStore> = Arc::new(MockRateLimitStore::new());

        let service = RateLimitService::with_store(
            tower::service_fn(ok_handler),
            Arc::clone(&store),
            Arc::clone(&config),
        );
        let first = service
            .oneshot(request_with_ip("203.0.113.2"))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);

        let service = RateLimitService::with_store(tower::service_fn(ok_handler), store, config);
        let second = service
            .oneshot(request_with_ip("203.0.113.2"))
            .await
            .unwrap();

        assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(second.headers().contains_key("retry-after"));
        assert_eq!(second.headers().get("RateLimit-Remaining").unwrap(), "0");
    }

    #[tokio::test]
    async fn different_ips_have_separate_counters() {
        let config = test_config(1);
        let store: Arc<dyn RateLimitStore> = Arc::new(MockRateLimitStore::new());

        let service = RateLimitService::with_store(
            tower::service_fn(ok_handler),
            Arc::clone(&store),
            Arc::clone(&config),
        );
        let first = service
            .oneshot(request_with_ip("203.0.113.10"))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);

        let service = RateLimitService::with_store(tower::service_fn(ok_handler), store, config);
        let second = service
            .oneshot(request_with_ip("203.0.113.20"))
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn dynamodb_error_returns_500() {
        let config = test_config(10);
        let store: Arc<dyn RateLimitStore> = Arc::new(MockRateLimitStore::failing());
        let service = RateLimitService::with_store(tower::service_fn(ok_handler), store, config);

        let response = service
            .oneshot(request_with_ip("203.0.113.30"))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn missing_ip_falls_through_to_inner() {
        let config = test_config(1);
        let store: Arc<dyn RateLimitStore> = Arc::new(MockRateLimitStore::new());
        let service = RateLimitService::with_store(tower::service_fn(ok_handler), store, config);

        let request = Request::builder()
            .method("GET")
            .uri("http://example.com/test")
            .body(Body::Empty)
            .unwrap();

        let response = service.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        // No RateLimit-* headers, because we skipped the counter entirely.
        assert!(!response.headers().contains_key("RateLimit-Limit"));
    }
}
