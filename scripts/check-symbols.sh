#!/usr/bin/env bash
# Gate 2.2, the half that only a linked binary can answer: no two libraries in
# the emulation build export the same BoringSSL symbol.
#
# The default tree already links aws-lc-sys, which rustls uses and which is a
# BoringSSL fork. The emulation build adds btls, which is BoringSSL. Both have
# an SSL_new, an EVP_DigestInit and several hundred more, and if either one
# exported them unprefixed the linker would pick a winner without saying so and
# the loser's callers would end up in the wrong implementation. aws-lc-sys
# renames its own; btls renames its own when the prefix-symbols feature is on,
# which crates/umi-fetch/Cargo.toml turns on for Linux targets.
#
# So the thing to assert is a property and not a spelling: after linking, no
# BoringSSL entry point is present under its bare name. Which prefix each crate
# chose is its own business and will change.
#
# Linux only, because objcopy is what does the rename and btls skips it
# elsewhere. Run it after scripts/check-tls.sh, which answers the cheaper half
# without building anything.
set -euo pipefail

cd "$(dirname "$0")/.."

if [ "$(uname -s)" != "Linux" ]; then
	echo "not linux, and btls only prefixes on linux, so there is nothing to check"
	exit 0
fi

# A handful of entry points both libraries define, picked to cover TLS, X.509
# and the digest layer rather than three names from the same file.
SYMBOLS="SSL_new SSL_CTX_new X509_free EVP_AEAD_CTX_seal ERR_clear_error"

echo "building the emulation test binary"
binary=$(cargo test -p umi-fetch --features emulation --test tls --no-run --message-format=json |
	python3 -c 'import json,sys
for line in sys.stdin:
    got = json.loads(line).get("executable")
    if got:
        print(got)
        break')

if [ -z "$binary" ]; then
	echo "  cargo did not say where it put the test binary"
	exit 1
fi
echo "  $binary"

fail=0
# To a file rather than a variable, and counted rather than matched with -q.
# `grep -q` stops at the first hit, the writer upstream gets SIGPIPE, and with
# `set -o pipefail` the pipeline then reports 141 whether or not there was a
# match. That turns this whole script into one that always says what you hoped
# to hear, which is the worst thing a gate can do.
table=$(mktemp)
trap 'rm -f "$table"' EXIT
nm -a "$binary" >"$table"

for name in $SYMBOLS; do
	# A bare name at the end of the line. Anything prefixed has characters in
	# front of it and anything suffixed, like the .cold partition gcc emits,
	# has characters after it.
	bare=$(grep -cE "[[:space:]]${name}\$" "$table" || true)
	if [ "$bare" -ne 0 ]; then
		printf '  %s is exported unprefixed, so two boringssl forks can collide\n' "$name"
		fail=1
	fi
	# And the other direction, so that a build which quietly stopped linking
	# BoringSSL at all cannot pass by having none of these symbols.
	prefixed=$(grep -cE "_${name}\$" "$table" || true)
	if [ "$prefixed" -eq 0 ]; then
		printf '  %s is not in the binary at all, prefixed or otherwise\n' "$name"
		fail=1
	fi
done

if [ "$fail" -ne 0 ]; then
	echo
	echo "gate 2.2 failed, see docs/spec/05-fetch-tiers.md section 5.5"
	exit 1
fi

echo "ok"
