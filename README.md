# rust-lambda-middleware-example

Companion code for the blog post [**Writing middlewares for Rust Lambda functions**](https://loige.co/writing-middlewares-for-rust-lambda-functions/) on [loige.co](https://loige.co).

A minimal AWS Lambda in Rust that shows how to build reusable middleware with [tower](https://crates.io/crates/tower) (the generic middleware engine that already underpins the Rust Lambda runtime). The worked example is a DynamoDB-backed IP rate limiter that:

- keys requests on client IP (extracted with sensible header priority),
- uses a fixed window configurable via `RATE_LIMIT_WINDOW_SECS`,
- exposes standard `RateLimit-Limit`, `RateLimit-Remaining`, `RateLimit-Reset` response headers,
- returns a plain JSON `429` with `Retry-After` when the limit is exceeded,
- uses a DynamoDB atomic counter with TTL-based cleanup.

## Layout

```
src/
  main.rs          - hello-world handler wired with ServiceBuilder
  ip_extractor.rs  - client IP extraction (X-Forwarded-For / X-Real-IP / CF-Connecting-IP)
  rate_limit.rs    - Tower Layer + Service with DynamoDB-backed counter
template.yaml      - SAM template (DynamoDB table + Lambda + HTTP API)
```

## Build and deploy

Requires [cargo-lambda](https://www.cargo-lambda.info/) and the [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html).

```sh
sam build
sam deploy --guided
```

## Try it

After deploy, copy the `HelloApi` endpoint from the stack outputs, then:

```sh
for i in (seq 1 12); curl -i $URL; end
```

The first 10 requests should return `200` with decrementing `RateLimit-Remaining`. The 11th returns `429` with `Retry-After`.

## License

MIT
