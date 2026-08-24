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
#   scripts/remote-check.sh server3 UMI_BLESS=1 test -p umi-extract
#
# Arguments before the cargo subcommand that look like NAME=value are exported
# on the server, which is how the golden corpus gets blessed and how the bench
# is pointed at a directory of real pages.
#
# The servers run other work and a benchmark that shares a cpu measures that
# other work, so PREFIX puts something in front of cargo:
#
#   PREFIX=nice scripts/remote-check.sh server3 bench -p umi-extract
#
# For the real thing, compile first and then run the binary on its own, pinned
# to one core at real time priority so it takes that cpu instead of queueing for
# it. Do not put cargo itself under `chrt`: rustc is parallel and would inherit
# real time priority on every core, on a machine running a live crawl.
#
#   scripts/remote-check.sh server3 bench -p umi-extract --no-run
#   ssh server3 'cd umi-build && taskset -c 7 chrt --fifo 50 ./target/release/deps/extract-* --bench'
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

while [ "$#" -gt 0 ] && [[ "$1" == [A-Z_]*=* ]]; do
	env="$env $1"
	shift
done

prefix="${PREFIX:-}"

if [ "$#" -gt 0 ]; then
	ssh "$host" "$env; cd $remote && time $prefix cargo $*"
	exit
fi

ssh "$host" "$env; cd $remote && set -x &&
	time cargo fmt --all -- --check &&
	time cargo clippy --workspace --all-targets --all-features -- -D warnings &&
	time cargo test --workspace &&
	RUSTDOCFLAGS='-D warnings' time cargo doc --workspace --no-deps"
