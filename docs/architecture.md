# Connector SDK Architecture

## Decision

Build a narrow, reusable HTTP connector SDK around Tower 0.5, governor 0.10,
and reqwest 0.13 with rustls.

The SDK is not security-specific despite the repository name. It owns the
outbound HTTP and sync-state seams that connector authors repeatedly need; it
does not own the application lifecycle around them.

## Reuse-first result

The Rust tooling baseline was reviewed before implementation. It already chose
the transport substrate but did not name an ingestion framework. An executable
spike then evaluated `faucet-core` and `faucet-source-rest`.

Faucet was accepted as evidence for the right ingestion concepts and rejected
as the SDK core for one load-bearing reason: its REST source privately owns the
reqwest client and pagination loop. A single source call can issue multiple
HTTP requests, so wrapping that call cannot apply Tower and governor policy to
each request. The spike and its detailed acceptance results remain in
`spikes/faucet-adoption`.

This decision avoids rebuilding Faucet's state stores, sinks, pipelines, CLI,
or runtime while preserving the required transport boundary.

## Component boundaries

| Component | Owns | Does not own |
| --- | --- | --- |
| `Source` | request construction, response validation, record decoding, checkpoint derivation | credentials, retries, rate limits, storage |
| `Authenticator` | attaching or asynchronously refreshing request credentials, rejecting insecure destinations | authorization policy, credential persistence |
| `Pagination` | applying and deriving page cursors | durable resume state |
| `RetryPolicy` | retry classification, attempts, backoff, jitter, `Retry-After` | job-level retry or dead-letter behavior |
| `GovernorLayer` | shared outbound request pacing | distributed/global quotas |
| `HttpClient` | type-erased Tower request execution and finite default deadlines | source or domain behavior |
| `SyncSession` | one run's cursor progression and loop detection | checkpoint persistence, sinks, scheduling |

The request path is:

```text
SyncSession
  -> Source::build_request
  -> Authenticator
  -> RetryPolicy
  -> GovernorLayer
  -> reqwest::Client
  -> Source::decode_page
  -> Page<Record, Cursor, Checkpoint>
```

The default layer order authenticates once per logical request and rate-limits
every physical attempt. Retries reuse the authenticated request snapshot, so a
refreshing authenticator must return a credential expected to remain valid for
the configured retry sequence. Unauthorized responses are returned to the
connector rather than retried. Connector authors can obtain the concrete
service with `HttpClientBuilder::build_service`, add Tower layers that preserve
`HttpError`, and type-erase the result using `HttpClient::from_service`.

The SDK-built reqwest client has a 10-second connect timeout, 30-second
read-idle timeout, and 120-second total deadline. Automatic redirects are
disabled: redirect targets must become new source requests and cross the
authentication boundary again. A caller-injected reqwest client deliberately
owns its own timeouts and redirect policy.

## State and durability contract

There are two deliberately different state types:

- A cursor is ephemeral. It advances within one session and is used only to
  fetch the next page.
- A checkpoint is durable. It represents source progress that can resume a
  later session.

`SyncSession::new` begins without a checkpoint. The checkpoint passed to
`SyncSession::resume` remains unchanged for all pages in that session. A source
may emit a newer checkpoint on each decoded page, but the SDK never saves it.
The caller must first durably consume that page's records and only then persist
its checkpoint. A crash before the checkpoint save may replay records; a crash
cannot cause records to be skipped by an eager SDK checkpoint write.

Connectors and sinks should therefore be replay-tolerant or idempotent. Exact
once delivery is outside this SDK because it requires a transaction spanning
the caller's sink and checkpoint store.

## Failure behavior

- Returning the cursor used for the current request fails with
  `IngestError::RepeatedCursor`. Longer cycles such as A -> B -> A are not
  detected because cursors are not required to implement `Hash` and `Eq`.
- Retry defaults to standard idempotent methods. Retrying POST requires an
  explicit `RetryMode::AllMethods` choice by the connector author.
- Only connection, timeout, request-body transport failures, `429`, and `5xx`
  responses are retryable. `Retry-After` never exceeds the configured maximum
  backoff.
- A request body that reqwest cannot clone is attempted once.
- Authentication header values are marked sensitive for downstream HTTP
  diagnostics.
- Credential authenticators reject non-loopback plaintext HTTP by default.
  HTTPS and loopback HTTP are accepted; `allow_insecure_http()` is an explicit
  controlled-lab opt-out. Custom authenticators inherit the fail-closed policy.
- Default connect, read-idle, and total request timeouts prevent stalled peers
  from suspending a session indefinitely.
- Source-specific construction and decoding errors remain typed inside
  `IngestError`.

## Extension points

- Implement `Authenticator` for OAuth refresh, request signing, or other
  provider-specific credentials, accounting for its once-per-logical-request
  execution semantics.
- Implement `Pagination` for link headers, continuation tokens, or compound
  cursors.
- Wrap the concrete default service with tracing, metrics, concurrency, or
  other error-preserving Tower layers.
- Override `HttpTimeouts`, or inject a reqwest client when the caller must own
  timeout and redirect behavior.
- Implement `Source` with application-specific records and checkpoints.

Persistence adapters, distributed rate limiting, sink transactions,
orchestration, connector registries, and security-domain validation remain
outside the crate until a concrete cross-connector requirement justifies them.
