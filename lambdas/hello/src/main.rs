//! Hello-world Lambda that wires up the [`RateLimitLayer`] from this
//! crate's library.
//!
//! Build with `sam build`, deploy with `sam deploy --guided`. See the
//! repo `README.md` for the full walkthrough.

use std::time::Duration;

use lambda_http::tower::ServiceBuilder;
use lambda_http::{run, service_fn, tracing, Error};
use tower_http::cors::CorsLayer;

use rust_lambda_middleware_example::{RateLimitConfig, RateLimitLayer};

mod http_handler;
use http_handler::function_handler;

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing::init_default_subscriber();

    let aws_config = aws_config::load_from_env().await;
    let dynamodb_client = aws_sdk_dynamodb::Client::new(&aws_config);

    let table_name =
        std::env::var("RATE_LIMIT_TABLE_NAME").expect("RATE_LIMIT_TABLE_NAME must be set");
    let max_requests: u32 = std::env::var("RATE_LIMIT_MAX_REQUESTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);
    let window_secs: u64 = std::env::var("RATE_LIMIT_WINDOW_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(900);
    let window_duration = Duration::from_secs(window_secs);

    let rate_limit = RateLimitLayer::new(
        RateLimitConfig {
            table_name,
            max_requests,
            window_duration,
        },
        dynamodb_client,
    );

    // CorsLayer sits on the outside so even rate-limited 429s carry
    // the right CORS headers, and the rate limiter sits between it
    // and the handler so the handler stays focused on business logic.
    let service = ServiceBuilder::new()
        .layer(CorsLayer::permissive())
        .layer(rate_limit)
        .service(service_fn(function_handler));

    // For Lambda Managed Instances with PerExecutionEnvironmentMaxConcurrency
    // > 1, swap `run` for `lambda_http::run_concurrent` and enable the
    // `concurrency-tokio` feature on `lambda_http`. For classic Lambda (one
    // event per execution environment) the two behave identically.
    run(service).await
}
