use lambda_http::tower::ServiceBuilder;
use lambda_http::{run, service_fn, tracing, Body, Error, Request, Response};
use serde_json::json;

mod ip_extractor;
mod rate_limit;

use rate_limit::{RateLimitConfig, RateLimitLayer};

async fn handler(_request: Request) -> Result<Response<Body>, Error> {
    let body = json!({ "message": "hello, rusty middleware" }).to_string();
    Ok(Response::builder()
        .status(200)
        .header("content-type", "application/json")
        .body(body.into())?)
}

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

    let rate_limit = RateLimitLayer::new(
        RateLimitConfig {
            table_name,
            max_requests,
            window_secs,
        },
        dynamodb_client,
    );

    let service = ServiceBuilder::new()
        .layer(rate_limit)
        .service(service_fn(handler));

    // For Lambda Managed Instances, swap `run` for `lambda_http::run_concurrent`
    // and enable the `concurrency-tokio` feature on `lambda_http`. On classic
    // Lambda (AWS_LAMBDA_MAX_CONCURRENCY <= 1) the two behave identically.
    run(service).await
}
