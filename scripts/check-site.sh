#!/usr/bin/env bash
# The bot page, checked against the code rather than against memory.
#
# Doc 07.1 says the page at umi.dev/bot is a deliverable and not a nicety,
# and the thing that makes it worthless is drift: an address list that has
# fallen behind the binary, a robots token that no longer matches what the
# parser looks for, a user agent string that is one character off. Every one
# of those turns the page from an answer into a wrong answer, so each one is
# checked here.
#
# The same house style as docs/spec applies, since it is the same writing.
set -euo pipefail

cd "$(dirname "$0")/.."
site=site
bot=$site/bot/index.html
fail=0

note() {
	printf '  %s\n' "$1"
	fail=1
}

if [ ! -d "$site" ]; then
	echo "no $site directory"
	exit 1
fi

echo "the published address list matches the one in the binary"
if ! diff -q identity/umi.json "$site/bot/umi.json" > /dev/null; then
	note "site/bot/umi.json and identity/umi.json differ, and the binary serves the second one"
fi

echo "every published address is on the page"
while read -r addr; do
	[ -n "$addr" ] || continue
	grep -qF "$addr" "$bot" || note "$addr is published but is not on the bot page"
done < <(grep -o '"ipv[46]Prefix": *"[^"/]*' identity/umi.json | sed 's/.*"//')

echo "every published name is on the page"
while read -r name; do
	[ -n "$name" ] || continue
	grep -qF "$name" "$bot" || note "$name is published but is not on the bot page"
done < <(grep -o '"name": *"[^"]*"' identity/umi.json | sed 's/.*: *"//; s/"$//' | sort -u)

echo "the user agent string matches the one requests carry"
agent=$(grep -o 'pub const USER_AGENT: &str = "[^"]*"' crates/umi-fetch/src/lib.rs | sed 's/.*= "//; s/"$//')
if [ -z "$agent" ]; then
	note "could not find USER_AGENT in crates/umi-fetch/src/lib.rs"
elif ! grep -qF "$agent" "$bot"; then
	note "the page does not carry the user agent string $agent"
fi

echo "the robots token matches the one the parser matches on"
# Doc 07.4 fixes the token at `umi` and says the page states it exactly.
# An operator who copies a wrong token off this page gets a robots.txt that
# does nothing, which is the worst outcome on the page.
if ! grep -qF 'User-agent: umi' "$bot"; then
	note "the page does not show a robots.txt group for the token umi"
fi
if ! grep -qF '<code>umi</code>' "$bot"; then
	note "the page does not state the bare token umi"
fi

echo "the purpose declaration agrees with the page"
for pair in \
	'"robots_token": "umi"' \
	'"trains_models": false' \
	'"acts_as_agent": false' \
	'"resells_access": false'; do
	grep -qF "$pair" "$site/bot/purpose.json" || note "purpose.json is missing $pair"
done
grep -qF "\"user_agent\": \"$agent\"" "$site/bot/purpose.json" ||
	note "purpose.json does not carry the user agent string $agent"

echo "the supervised list says what the code does"
# Doc 05.7's T4 is the one tier a person has to type a command to reach, and
# the page promising that only holds while the code agrees. Two things can
# drift and both are checked: an entry count on the page that is not the
# length of the list beside it, and the claim that the supervised browser is
# not built, which stops being true the day somebody builds it.
supervised=$site/bot/supervised.json
python3 - "$supervised" <<'PY' || fail=1
import json, sys

doc = json.load(open(sys.argv[1]))
ok = True
if doc["count"] != len(doc["entries"]):
	print("  supervised.json says %d entries and lists %d" % (doc["count"], len(doc["entries"])))
	ok = False
for entry in doc["entries"]:
	for field in ("domain", "operator", "reason", "added"):
		if not str(entry.get(field, "")).strip():
			print("  a supervised entry has no %s on it" % field)
			ok = False
sys.exit(0 if ok else 1)
PY
if grep -qF '"engine_built": false' "$supervised" &&
	grep -rqF 'Tier::Supervised => ' crates/umi-fetch/src/lib.rs; then
	note "supervised.json says the browser is not built and the ladder has a rung for it"
fi
grep -qF '/bot/supervised.json' "$bot" || note "the bot page does not link the supervised list"

echo "the json parses"
for f in "$site"/bot/*.json; do
	python3 -c "import json,sys; json.load(open(sys.argv[1]))" "$f" ||
		note "$f is not valid json"
done

echo "internal links resolve"
while read -r target; do
	[ -n "$target" ] || continue
	path=$site$target
	if [ -d "$path" ]; then
		path=$path/index.html
	elif [ ! -f "$path" ] && [ -f "$path/index.html" ]; then
		path=$path/index.html
	fi
	[ -f "$path" ] || note "a link to $target has nothing behind it"
done < <(grep -o 'href="/[^"]*"' "$site"/*.html "$site"/bot/*.html | sed 's/.*href="//; s/"$//' | sort -u)

echo "em dashes and en dashes"
if hits=$(grep -rn $'[—–]' "$site" 2>/dev/null); then
	note "$hits"
fi

echo "trailing whitespace"
if hits=$(grep -rn ' $' "$site" 2>/dev/null); then
	note "$hits"
fi

if [ "$fail" -ne 0 ]; then
	echo
	echo "site check failed"
	exit 1
fi

echo "site check passed"
