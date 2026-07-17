# Faucet Adoption Spike

This package evaluates whether `faucet-core` and `faucet-source-rest` can
replace a new Custos-owned connector and ingestion SDK.

## Acceptance criteria

- Embed the source without the Faucet CLI or runtime service.
- Authenticate and paginate a REST source using deterministic HTTP fixtures.
- Stream connector-specific Rust records without buffering the entire source.
- Preserve Faucet bookmarks at the typed boundary.
- Resume an incremental sync from a durable state store.
- Identify whether Tower middleware and governor can be applied per request.

## Scope

The package is a disposable compatibility probe. It is not a public Custos API
and is not publishable. Dependency versions are exact so a future rerun tests
the same Faucet release reviewed by the spike.

## Commands

```console
cargo test -p custos-connect-faucet-spike
cargo clippy -p custos-connect-faucet-spike --all-targets -- -D warnings
```

## Results

| Criterion | Result | Evidence |
| --- | --- | --- |
| Embeddable without the CLI | Pass | Tests construct `RestStream` and `Pipeline` directly. |
| Authentication and pagination | Pass | A two-page bearer-authenticated cursor feed completes. |
| Typed streaming | Pass with adapter | `typed_pages` decodes each page without buffering the full source. |
| Checkpoint preservation | Pass | Only the terminal REST page carries the maximum replication key. |
| Durable incremental resume | Pass | A second pipeline run loads the stored bookmark and emits only the newer record. |
| Per-request Tower middleware | Fail | `RestStream` privately constructs and owns its `reqwest::Client`. |
| Per-request governor limiting | Fail | The private pagination loop cannot be wrapped at the HTTP request boundary. |

The spike passes `cargo fmt`, all four tests, Clippy with warnings denied,
doctests, and rustdoc with warnings denied. Its normal dependency graph contains
none of the crates banned by the Rust tooling baseline.

## Verdict

Reject direct adoption of Faucet as the core connector SDK.

This is a scoped architectural rejection, not a rejection of its ingestion
behavior. Faucet already solves broad ETL orchestration, state stores, sinks,
and JSON-oriented source composition; Custos should not recreate those layers.
The hard mismatch is the outbound transport seam: the proposed SDK requires
every paginated request to pass through a caller-composable `tower::Service`
stack over reqwest, with governor rate limiting and retry policy applied at that
boundary. Wrapping Faucet's `Source` is too coarse because one source call can
issue many requests internally.

Proceed with a narrower owned SDK containing only:

- a page-and-checkpoint `Source` contract;
- an injectable Tower service for reqwest requests and responses;
- reusable auth, pagination, retry classification, and governor layers; and
- adapters that leave persistence, sinks, orchestration, and domain validation
  to their callers.

Keep this spike as an executable comparison test while designing that API. A
future Faucet release that exposes its request executor as a Tower service can
be reevaluated without changing the decision criteria.
