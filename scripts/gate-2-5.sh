#!/usr/bin/env bash
# Doc 16's gate 2.5, started. It runs for fourteen days.
#
# Two processes, both under nohup, both writing into one directory:
#
#   the reference clock   gate-2-5-poll.py, one feed fetch every five minutes
#   the crawl             umi crawl --watch on the same domain
#
# Neither one knows about the other, which is the point. The reference clock
# records when the publisher said a story exists and the crawl records when we
# had a row for it, and `gate-2-5.py` is the only thing that sees both.
#
# Fourteen days rather than two, because doc 16 says so and the reason is
# good: a revisit schedule that looks right for a day is a schedule that has
# not yet had to decide what to do with the pages it deprioritised.
#
# Start it on a server and walk away:
#
#     ./scripts/remote-check.sh server1 build --release -p umi-cli
#     ssh server1 'cd umi-build && ./scripts/gate-2-5.sh'
#
# Then on day fourteen:
#
#     ./scripts/gate-2-5-cc.py --reference OUT/reference.jsonl \
#         --domain aljazeera.com --out OUT/commoncrawl.jsonl
#     ./scripts/gate-2-5.py --reference OUT/reference.jsonl \
#         --crawl OUT/crawl --cc OUT/commoncrawl.jsonl --umi ./target/release/umi
set -euo pipefail

# Al Jazeera because the gate needs a news domain that we are actually allowed
# to crawl and that Common Crawl is allowed to crawl too, and there are fewer
# of those than there used to be. Its robots.txt gives `*` everything except
# the search and api paths, it does not block CCBot, so the comparison the
# gate asks for is possible, and it publishes a full text feed with a real
# pubDate on every item. The obvious alternatives each fail one of those:
# theregister.com disallows `*` outright, cbc.ca and arstechnica.com block
# CCBot so there is nothing to compare against, phoronix.com blocks both, and
# theguardian.com's robots.txt asks in plain English not to be used for
# machine learning, which is a request worth honouring whatever the rules say.
DOMAIN="${DOMAIN:-www.aljazeera.com}"
FEED="${FEED:-https://www.aljazeera.com/xml/rss/all.xml}"
CC_DOMAIN="${CC_DOMAIN:-aljazeera.com}"

OUT="${OUT:-$HOME/gate-2-5}"
# Five minutes. The gate is a six hour median, so the reference clock has to be
# finer than that by a wide margin or it becomes the thing being measured. It
# is also one request every five minutes to one host, which is a rate no news
# site will notice.
EVERY="${EVERY:-300}"
DAYS="${DAYS:-14}"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

umi="$root/target/release/umi"
if [ ! -x "$umi" ]; then
	echo "building"
	cargo build --release -p umi-cli
fi

mkdir -p "$OUT"

if [ -f "$OUT/poll.pid" ] && kill -0 "$(cat "$OUT/poll.pid")" 2> /dev/null; then
	echo "the reference clock is already running as $(cat "$OUT/poll.pid")"
else
	nohup "$root/scripts/gate-2-5-poll.py" \
		--feed "$FEED" \
		--out "$OUT/reference.jsonl" \
		--every "$EVERY" \
		--days "$DAYS" \
		>> "$OUT/poll.log" 2>&1 &
	echo $! > "$OUT/poll.pid"
	echo "reference clock started as $(cat "$OUT/poll.pid"), every ${EVERY}s for $DAYS days"
fi

if [ -f "$OUT/crawl.pid" ] && kill -0 "$(cat "$OUT/crawl.pid")" 2> /dev/null; then
	echo "the crawl is already running as $(cat "$OUT/crawl.pid")"
else
	# `--watch` rather than a plain crawl, because the gate is about what
	# happens after the frontier drains. A crawl that stops when it has seen
	# everything has measured discovery once and nothing else, and the number
	# the gate wants is what the revisit schedule does on day nine.
	#
	# No `--publish`. Doc 12 deletes the local copy of a segment once it is
	# published and verified, and the local copy is what `gate-2-5.py` reads.
	# Publishing this run is a separate decision and it can be made afterwards.
	nohup "$umi" crawl "https://$DOMAIN/" \
		--watch \
		--out "$OUT/crawl" \
		>> "$OUT/crawl.log" 2>&1 &
	echo $! > "$OUT/crawl.pid"
	echo "crawl started as $(cat "$OUT/crawl.pid") into $OUT/crawl"
fi

echo
echo "watch it with:"
echo "  tail -f $OUT/crawl.log"
echo "  wc -l $OUT/reference.jsonl"
echo
echo "stop it with:"
echo "  kill \$(cat $OUT/poll.pid) \$(cat $OUT/crawl.pid)"
echo
echo "the crawl takes a SIGTERM and writes what it has, so stopping it early"
echo "leaves a directory the analysis can still read."
