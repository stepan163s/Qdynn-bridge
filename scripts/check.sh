#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
node --check crates/qdynn-server/src/bridge.js
node tests/bridge-startup.cjs
node tests/bridge-deadlines.cjs
cargo build --locked -p qdynn-server
python3 tests/smoke-binary.py target/debug/qdynn-bridge
node tests/bridge-recovery-interop.cjs target/debug/qdynn-bridge
node tests/bridge-recovery-interop.cjs target/debug/qdynn-bridge --runtime-fallback
python3 tests/doctor-smoke.py target/debug/qdynn-bridge
python3 tests/deploy-smoke.py target/debug/qdynn-bridge
