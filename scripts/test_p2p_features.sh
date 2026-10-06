#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "${repo_root}"

cargo test --jobs "$(sysctl -n hw.ncpu)" -p rustfs --features p2p p2p_primitives
