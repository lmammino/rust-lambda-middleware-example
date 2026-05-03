//! Library crate for the `rust-lambda-middleware-example` companion repo
//! (see the blog post at <https://loige.co/writing-middlewares-for-rust-lambda-functions/>).
//!
//! The crate exposes:
//!
//! * [`ip_extractor`] — a small helper that pulls the client IP out of an
//!   incoming Lambda HTTP request.
//! * [`rate_limit`] — a tower middleware that enforces a per-IP fixed-window
//!   rate limit backed by DynamoDB.
//!
//! The deployable hello-world Lambda lives at `src/bin/hello.rs` and consumes
//! this library, and a handful of standalone tower middleware demos live under
//! `examples/`.

pub mod ip_extractor;
pub mod rate_limit;

pub use ip_extractor::extract_ip;
pub use rate_limit::{OverLimitCtx, OverLimitFn, RateLimitConfig, RateLimitLayer, UnavailableFn};
