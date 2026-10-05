# custos-connect

`custos-connect` is a domain-neutral Rust SDK for HTTP ingestion connectors. It
keeps source-specific request and decoding logic separate from reusable
authentication, retry, rate limiting, pagination, and incremental-sync state.

The transport substrate is deliberately small and reuse-first: Tower 0.5 for
middleware composition, governor 0.10 for outbound quotas, and reqwest 0.13
over rustls. Native TLS is excluded. The SDK does not introduce a
security-specific data model or orchestration runtime.

## Workspace

- [`crates/custos-connect`](crates/custos-connect) contains the publishable SDK.
- [`docs/architecture.md`](docs/architecture.md) defines its boundaries and
  state guarantees.
- [`spikes/faucet-adoption`](spikes/faucet-adoption) is the executable
  reuse-first evaluation that preceded the owned API.

The Faucet spike confirmed that its REST source already handles much of the
ingestion lifecycle, but it cannot expose each paginated request through the
required caller-composable Tower/governor service boundary. The SDK therefore
owns only that narrower connector seam and leaves persistence, sinks, and job
orchestration to applications.

## Minimum Rust version

The publishable SDK minimum is Rust 1.94.0, aligned with Custos core. Its CI
MSRV job reads the exact declared version and tests every SDK target and feature
with the committed lockfile. The unpublished Faucet evaluation binary separately
declares Rust 1.96.0 because its pinned Faucet 1.4.0/1.2.2 dependencies require it;
a separate exact-toolchain job tests every evaluation target and feature. Stable
formatting, lint, tests, documentation and supply-chain checks still cover the
whole workspace. No dependency or license is changed to lower the SDK minimum.
Source acceptance requires the checks to pass at the reviewed head; library CI
does not establish a Custos consumer adoption or a published release.

## Verify

```console
cargo fmt --all -- --check
cargo test --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --doc --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
cargo package -p custos-connect --locked
```

## Licensing

This shared library is dual-licensed under `MIT OR Apache-2.0`, at your option.
See [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE).
