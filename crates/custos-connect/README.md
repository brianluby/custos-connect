# custos-connect

A small, domain-neutral SDK for building paginated and incremental HTTP
ingestion sources in Rust.

## Responsibilities

The crate provides:

- a typed `Source` contract for request construction and page decoding;
- a `SyncSession` that advances ephemeral page cursors;
- caller-owned durable checkpoints for incremental sync;
- async bearer, API-key, Basic, or custom authentication;
- idempotency-aware retry with exponential backoff, jitter, and `Retry-After`;
- bounded connect, read-idle, and total request timeouts;
- governor rate limiting shared across cloned Tower services; and
- cursor, page-number, and custom pagination strategies.

It intentionally does not provide storage, sinks, scheduling, job execution,
domain records, or connector discovery.

## Execution model

The default outbound stack is:

```text
Source -> authentication -> retry -> governor -> reqwest
```

Authentication runs once per logical request, before the retry layer clones
the authenticated request. Retries reuse that snapshot and do not invoke the
authenticator again. Refreshing authenticators must therefore acquire a
credential expected to remain valid for the configured retry sequence; `401`
responses are returned to the connector rather than retried. Every retry
consumes governor capacity. The default retry mode permits only idempotent HTTP
methods and only retries transient transport failures, `429`, and `5xx`
responses. Reqwest streaming request bodies are not cloneable and therefore
run once.
Server-provided `Retry-After` delays are bounded by the configured maximum
backoff so an upstream cannot suspend a connector indefinitely.

The SDK-built reqwest client defaults to a 10-second connect timeout, a
30-second read-idle timeout, and a 120-second total request deadline. It does
not automatically follow redirects, so every destination passes through the
Tower and authentication boundary. `HttpTimeouts` changes the defaults;
`with_reqwest_client` transfers timeout and redirect-policy ownership to the
caller.

Credential authenticators permit HTTPS and loopback HTTP by default. Remote
plaintext HTTP fails with `AuthError::InsecureTransport` before credentials are
attached. `allow_insecure_http()` is the explicit opt-out for controlled labs.

Create the default transport explicitly:

```rust
use std::num::NonZeroU32;

use custos_connect::{HttpClientBuilder, Quota, RetryPolicy};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let quota = Quota::per_second(NonZeroU32::MIN);
    let retry = RetryPolicy::new(NonZeroU32::MIN);
    let _client = HttpClientBuilder::new(quota, retry).build()?;
    Ok(())
}
```

`HttpClientBuilder::build_service` exposes the concrete Tower stack before
type erasure. Apply additional error-preserving Tower layers there, then use
`HttpClient::from_service` to pass the result to a `SyncSession`. Both `build`
methods are fallible so reqwest or TLS initialization errors are propagated.

## Source contract

A source builds a reqwest `Request` from its current `SyncPosition` and decodes
the response into a typed `Page`. The position contains:

- the ephemeral cursor for the current paginated run; and
- the durable resume checkpoint supplied when the session started.

The resume checkpoint is intentionally stable for the entire session. A page
may return both the next cursor and a candidate checkpoint:

Use `SyncSession::new(source, client)` for a fresh sync and
`SyncSession::resume(source, client, checkpoint)` to resume durable progress.

```rust,ignore
while let Some(page) = session.next_page().await? {
    let (records, _next_cursor, checkpoint) = page.into_parts();
    sink.write(records).await?;

    // Persist only after the records have been durably consumed.
    if let Some(checkpoint) = checkpoint {
        state.save(checkpoint).await?;
    }
}
```

`SyncSession` never persists checkpoints. This makes the durability boundary
explicit and prevents advancing state ahead of the records it represents.

See the workspace integration tests for a complete bearer-authenticated,
cursor-paginated source.

## Licensing

This shared library is dual-licensed under `MIT OR Apache-2.0`, at your option.
See [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE).
