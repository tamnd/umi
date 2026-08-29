#!/usr/bin/env bash
# Doc 16's gate 2.3, politeness under an origin that is trying to break it.
#
# Starts the adversarial origin from `crates/umi-crawl/examples`, crawls it,
# kills the crawler halfway through and starts it again on the same state, then
# hands the origin's arrival log to `scripts/check-politeness.py`. Nothing here
# reads the crawler's own output, which is the point: the gate is about what the
# server saw.
#
# The kill is a SIGKILL rather than a SIGTERM on purpose. A graceful stop could
# write the host's delay out on the way down and the run would prove nothing
# about where that number lives. Doc 07.6 puts the politeness timer in the state
# store on every completion, so a crawler that is shot in the head and started
# again has to come back still backed off.
#
# Run it on a server rather than a laptop:
#
#     ./scripts/remote-check.sh server3 build --release -p umi-cli
#     ssh server3 'cd umi-build && ./scripts/gate-2-3.sh'
set -euo pipefail

PORT="${PORT:-8099}"
OUT="${OUT:-/tmp/gate-2-3}"
LOG="${LOG:-/tmp/adversarial-origin.tsv}"
# How long the first crawler gets before it is killed. Long enough to be well
# past the front page and into the backoff, short enough that the second half of
# the run is still most of the run.
FIRST_SECS="${FIRST_SECS:-45}"
# How long the second crawler gets. This one is a plan and not a ceiling. The
# origin fails one request in five and doc 07.6 is asymmetric, four times up
# against a ninth down, so a site like this pins the crawler at the sixty
# second ceiling and stays there. The frontier never drains and it is not meant
# to: what the log needs is every behaviour, a restart in the middle of it, and
# long enough after the ceiling to show the rate holding. Ten minutes is about
# three times what that takes.
SECOND_SECS="${SECOND_SECS:-600}"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

echo "building"
cargo build --release -p umi-cli
cargo build --release -p umi-crawl --example adversarial_origin

umi="$root/target/release/umi"
origin="$root/target/release/examples/adversarial_origin"

rm -rf "$OUT"
mkdir -p "$OUT"

"$origin" --port "$PORT" --log "$LOG" &
origin_pid=$!
trap 'kill "$origin_pid" 2>/dev/null || true' EXIT

# Wait for the listener by opening a socket and closing it. A GET would land in
# the log and every gap after it would be measured against a request the crawler
# never made.
for _ in $(seq 1 100); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
        exec 3>&- 2>/dev/null || true
        break
    fi
    sleep 0.1
done

# The same crawl twice, so the restart is a restart rather than a second
# experiment. No --max-pages: what ends the run is the clock, and a page budget
# would end it somewhere that depends on how the backoff happened to land.
args=(crawl "http://127.0.0.1:$PORT/" --out "$OUT" --no-sitemaps --no-render --depth 8)

echo "first crawler, $FIRST_SECS seconds then SIGKILL"
"$umi" "${args[@]}" >"$OUT/first.log" 2>&1 &
crawler_pid=$!
sleep "$FIRST_SECS"
kill -9 "$crawler_pid" 2>/dev/null || true
wait "$crawler_pid" 2>/dev/null || true
restart_at="$(python3 -c 'import time; print(int(time.time() * 1000))')"
echo "killed at $restart_at"

echo "second crawler, on the same state"
# The timeout is the normal way this ends, so its exit status is not news.
timeout "$SECOND_SECS" "$umi" "${args[@]}" >"$OUT/second.log" 2>&1 \
    || echo "second crawler stopped on the clock, which is what was meant to happen"

kill "$origin_pid" 2>/dev/null || true
trap - EXIT

echo
"$root/scripts/check-politeness.py" "$LOG" --restart-at "$restart_at"
