//! examples/log_layer_broken.rs
//!
//! Stage 2 of the log layer evolution. **This file does not compile on
//! purpose** and is gated behind the `intentionally-broken` Cargo feature
//! so the default build skips it. To reproduce the failure yourself:
//!
//!     cargo build --example log_layer_broken --features intentionally-broken
//!
//! ## What we are trying
//!
//! Stage 1 (`log_layer_request_only.rs`) showed that logging *before* the
//! inner service is a one-line change. The natural follow-up is: what if
//! we want to log the response **status** too? The status only exists
//! after the inner service has resolved, so the obvious move is to await
//! the inner future and then emit the log line:
//!
//! ```text
//! fn call(&mut self, request: Request<Body>) -> Self::Future {
//!     let method = request.method().clone();
//!     let path = request.uri().path().to_string();
//!     let response = self.inner.call(request).await?;     // <-- BREAK
//!     tracing::info!(
//!         method = %method,
//!         path = %path,
//!         status = %response.status(),
//!         "request"
//!     );
//!     Ok(response)                                         // <-- BREAK
//! }
//! ```
//!
//! ## Why it does not compile
//!
//! Run the command above and `rustc` reports:
//!
//! ```text
//! error[E0728]: `await` is only allowed inside `async` functions and blocks
//!    --> examples/log_layer_broken.rs:106:49
//!     |
//! 106 |         let response = self.inner.call(request).await?;
//!     |                                                 ^^^^^ only allowed inside `async` functions and blocks
//! ```
//!
//! `Service::call` is not an `async fn`; it returns a `Self::Future`.
//! We cannot `.await` inside a non-async function body. (If we silenced
//! the `await` and tried to keep going, we would also hit a return-type
//! mismatch: `type Future = S::Future` vs a body that now returns a
//! `Result<Response<Body>, S::Error>`. `rustc` short-circuits on the
//! `await` error before showing that one, but it is the same root cause.)
//!
//! ## The lesson
//!
//! You **cannot peek inside the inner future without producing a new
//! Future type**. As soon as the middleware needs to do post-response
//! work (logging the status, mutating headers, mapping errors), the
//! return type must change too. The next two stages show the two
//! standard ways to produce that new future:
//!
//! * `examples/log_layer_manual_poll.rs` — hand-roll a `LogFuture<F>`
//!   struct with a manual `Future` impl. Verbose but mechanical.
//! * `examples/log_layer.rs` — return a
//!   `Pin<Box<dyn Future<...> + Send>>` and write the body as
//!   `Box::pin(async move { … })`. One heap allocation per request,
//!   ergonomic body. This is the idiomatic shape and what every other
//!   middleware in this repo uses.

use std::task::{Context, Poll};

use http::{Request, Response};
use lambda_http::tower::{service_fn, Layer, Service, ServiceBuilder, ServiceExt};
use lambda_http::{tracing, Body};

#[derive(Clone)]
pub struct LogLayer;

impl<S> Layer<S> for LogLayer {
    type Service = LogService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        LogService { inner }
    }
}

pub struct LogService<S> {
    inner: S,
}

impl<S> Service<Request<Body>> for LogService<S>
where
    S: Service<Request<Body>>,
{
    type Response = S::Response;
    type Error = S::Error;
    // We naively keep `S::Future` as the future type. The body below will
    // try to do work *after* awaiting it, which is exactly what does not
    // compile.
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let method = request.method().clone();
        let path = request.uri().path().to_string();
        // ↓↓↓ this is the naive attempt that does not compile ↓↓↓
        let response = self.inner.call(request).await?;
        tracing::info!(
            method = %method,
            path = %path,
            status = %response.status(),
            "request"
        );
        Ok(response)
    }
}

async fn handler(_req: Request<Body>) -> Result<Response<Body>, std::convert::Infallible> {
    Ok(Response::builder()
        .status(200)
        .body(Body::from(r#"{"message":"hello"}"#))
        .unwrap())
}

#[tokio::main]
async fn main() {
    tracing::init_default_subscriber();

    let service = ServiceBuilder::new()
        .layer(LogLayer)
        .service(service_fn(handler));

    let request = Request::builder()
        .method("GET")
        .uri("http://example.com/hello")
        .body(Body::Empty)
        .unwrap();

    let response = service.oneshot(request).await.unwrap();

    println!("status: {}", response.status());
}
