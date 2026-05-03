//! examples/log_layer_manual_poll.rs
//!
//! Stage 3 of the log layer evolution: the working but verbose version of
//! `examples/log_layer_broken.rs`. We define our own `LogFuture<F>` that
//! wraps the inner service's future and the metadata we want to log, and
//! we hand-roll a `Future` impl on it that polls the inner future and
//! emits the log line on `Poll::Ready(Ok(_))`.
//!
//! Compared to Stage 1 (`log_layer_request_only.rs`), the new bits are:
//!
//! * a `LogFuture<F>` struct (uses `pin_project_lite!` so we do not have
//!   to write `unsafe` pin projection by hand),
//! * an `impl<F> Future for LogFuture<F>` block,
//! * a different `type Future = LogFuture<S::Future>` line in the
//!   `Service` impl,
//! * extra trait bounds on the `Service` impl so the inner future's
//!   output is `Result<Response<Body>, _>`.
//!
//! Compared to Stage 4 (`log_layer.rs`), this version trades roughly 30
//! extra lines of boilerplate (and a `pin-project-lite` dependency) for
//! avoiding one heap allocation per request. In a Lambda the allocation
//! cost is irrelevant, which is why the idiomatic Stage 4 just uses
//! `Box::pin(async move { … })`.
//!
//! Run with: `cargo run --example log_layer_manual_poll`

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use http::{Method, Request, Response};
use lambda_http::tower::{service_fn, Layer, Service, ServiceBuilder, ServiceExt};
use lambda_http::{tracing, Body};
use pin_project_lite::pin_project;

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
    S: Service<Request<Body>, Response = Response<Body>>,
{
    type Response = Response<Body>;
    type Error = S::Error;
    type Future = LogFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let method = request.method().clone();
        let path = request.uri().path().to_string();
        LogFuture {
            method,
            path,
            inner: self.inner.call(request),
        }
    }
}

pin_project! {
    pub struct LogFuture<F> {
        method: Method,
        path: String,
        #[pin]
        inner: F,
    }
}

impl<F, E> Future for LogFuture<F>
where
    F: Future<Output = Result<Response<Body>, E>>,
{
    type Output = Result<Response<Body>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.inner.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(response)) => {
                tracing::info!(
                    method = %this.method,
                    path = %this.path,
                    status = %response.status(),
                    "request"
                );
                Poll::Ready(Ok(response))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
        }
    }
}

async fn handler(_req: Request<Body>) -> Result<Response<Body>, Infallible> {
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
