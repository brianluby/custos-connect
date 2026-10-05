# Changelog

## Unreleased

### Changed

- Align the publishable SDK minimum supported Rust version with Custos core at
  Rust 1.94.0, with all SDK targets/features tested at that exact toolchain.
- Keep the unpublished Faucet evaluation at its actual Rust 1.96.0 minimum and
  add its own exact-toolchain target/feature check. Its pinned upstream releases
  require 1.96. Stable validation and supply-chain checks retain the complete
  workspace.
- Update `Cargo.lock` for the h2 and rustls advisory fixes, including their
  transitive dependencies. These dependency updates are separate from the SDK
  minimum Rust version adjustment.
