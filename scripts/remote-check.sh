#!/usr/bin/env bash
# Run the full check suite on a build server instead of the laptop.
#
# The laptop does not have the disk for a workspace target directory, and the
# servers are faster anyway, so the working tree is pushed to one of them and
# cargo runs there. Only source goes over: the target directory and the git
# directory both stay where they are.
#
# Usage: scripts/remote-check.sh [host] [cargo subcommand ...]
#   scripts/remote-check.sh                 # fmt, clippy, test, doc
#   scripts/remote-check.sh server2         # the same, elsewhere
#   scripts/remote-check.sh server3 test -p umi-fetch
set -euo pipefail

host="${1:-server3}"
shift || true
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
remote="umi-build"

rsync -az --delete \
	--exclude '.git' \
	--exclude 'target' \
	--exclude '.umi' \
	--exclude 'umi.toml' \
	"$root/" "$host:$remote/"

if [ "$#" -gt 0 ]; then
	# shellcheck disable=SC2016
	ssh "$host" 'export PATH="$HOME/.cargo/bin:$PATH"; cd '"$remote"' && cargo '"$*"
	exit
fi

ssh "$host" 'export PATH="$HOME/.cargo/bin:$PATH"; cd '"$remote"' && set -x &&
	cargo fmt --all -- --check &&
	cargo clippy --workspace --all-targets --all-features -- -D warnings &&
	cargo test --workspace &&
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps'
