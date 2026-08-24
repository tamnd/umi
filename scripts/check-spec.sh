#!/usr/bin/env bash
# The house style for docs/spec, checked rather than remembered.
#
# The spec is 18 documents written to one set of rules: plain english, no em
# dashes, no horizontal rules, and no hard wrapping in the middle of a
# sentence. Those rules survive exactly as long as something enforces them, so
# this runs in CI.
set -euo pipefail

cd "$(dirname "$0")/.."
spec=docs/spec
fail=0

note() {
	printf '  %s\n' "$1"
	fail=1
}

if [ ! -d "$spec" ]; then
	echo "no $spec directory"
	exit 1
fi

echo "em dashes and en dashes"
if hits=$(grep -rn $'[—–]' "$spec" 2>/dev/null); then
	note "$hits"
fi

echo "horizontal rules"
if hits=$(grep -rn '^\(---\|\*\*\*\|___\)$' "$spec" 2>/dev/null); then
	note "$hits"
fi

echo "trailing whitespace"
if hits=$(grep -rn ' $' "$spec" 2>/dev/null); then
	note "$hits"
fi

echo "index links resolve"
while read -r target; do
	[ -n "$target" ] || continue
	[ -f "$spec/$target" ] || note "00-index.md links $target, which does not exist"
done < <(grep -o '([0-9][0-9]-[a-z0-9-]*\.md)' "$spec/00-index.md" | sed 's/^(//; s/)$//' | sort -u)

echo "index covers every document"
for f in "$spec"/[0-9][0-9]-*.md; do
	base=$(basename "$f")
	[ "$base" = "00-index.md" ] && continue
	grep -q "$base" "$spec/00-index.md" || note "$base is not listed in 00-index.md"
done

if [ "$fail" -ne 0 ]; then
	echo
	echo "spec style check failed"
	exit 1
fi

echo "spec style check passed"
