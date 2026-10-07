#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "${repo_root}"

jobs="$(nproc 2>/dev/null || sysctl -n hw.ncpu)"
cargo test --jobs "${jobs}" -p rustfs --features p2p p2p
