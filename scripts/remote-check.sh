#!/usr/bin/env bash
# Run cargo on a build server instead of on the laptop.
#
# The laptop does not have the disk for a workspace target directory and the
# servers have more cores, so the working tree is pushed to one of them and
# cargo runs there. Only source goes over. The target directory stays on the
# server and is never deleted, so the first run pays for the dependency tree
# once and every run after it is incremental and takes seconds.
#
# Usage: scripts/remote-check.sh [host] [cargo subcommand ...]
#   scripts/remote-check.sh                          # fmt, clippy, test, doc
#   scripts/remote-check.sh server2                  # the same, elsewhere
#   scripts/remote-check.sh server3 test -p umi-fetch
set -euo pipefail

host="${1:-server3}"
shift || true
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
remote="umi-build"

# `--delete` removes files that were deleted locally, but the excludes keep it
# away from the target directory, so the cache survives.
rsync -az --delete \
	--exclude '.git' \
	--exclude 'target' \
	--exclude '.umi' \
	--exclude 'umi.toml' \
	"$root/" "$host:$remote/"

# Incremental compilation is on for the dev profile anyway, but say so, because
# a stray profile override in CI copied into a shell would turn it off quietly.
env='export PATH="$HOME/.cargo/bin:$PATH" CARGO_INCREMENTAL=1 CARGO_TERM_COLOR=never'

if [ "$#" -gt 0 ]; then
	ssh "$host" "$env; cd $remote && time cargo $*"
	exit
fi

ssh "$host" "$env; cd $remote && set -x &&
	time cargo fmt --all -- --check &&
	time cargo clippy --workspace --all-targets --all-features -- -D warnings &&
	time cargo test --workspace &&
	RUSTDOCFLAGS='-D warnings' time cargo doc --workspace --no-deps"
