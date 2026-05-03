//! HTTP handler for the hello-world Lambda. This is the only piece that
//! cares about request/response shape; the runtime wiring lives in
//! `main.rs`.

use lambda_http::{Body, Error, Request, Response};
use serde_json::json;

pub async fn function_handler(_request: Request) -> Result<Response<Body>, Error> {
    let body = json!({ "message": "hello from your friendly Rust Lambda function" }).to_string();
    Ok(Response::builder()
        .status(200)
        .header("content-type", "application/json")
        .body(body.into())?)
}
