#!/usr/bin/env bash
# Gate 2.2: the default build has one TLS stack, and it is rustls.
#
# Doc 05.5 puts T2 behind the `emulation` feature because wreq links BoringSSL,
# and BoringSSL shares symbol prefixes with openssl-sys: a tree holding both
# either fails to link or links and then segfaults in a way that looks like a
# miscompile. There is also a plainer reason to care, from doc 01: a static
# binary that dynamically links OpenSSL is not a static binary.
#
# The lockfile cannot answer this on its own any more. An optional dependency
# is still locked, so Cargo.lock has had btls in it since T2 landed and always
# will. What the lockfile can still say is that nothing pulls native TLS in by
# accident, and the test named `the_tree_is_rustls_only` keeps saying it. This
# script answers the other half, which is what a default build actually
# compiles, and only `cargo tree` knows that.
set -euo pipefail

cd "$(dirname "$0")/.."
fail=0

# The whole workspace, normal dependencies only. Build and dev dependencies are
# not linked into the binary, and cc pulling something in for a build script
# would be a false positive.
tree=$(cargo tree --workspace --edges normal --prefix none --format '{p}')

echo "default build, no boringssl"
for crate in btls btls-sys boring boring-sys tokio-boring; do
	if hits=$(printf '%s\n' "$tree" | grep -E "^${crate} v" || true); [ -n "$hits" ]; then
		printf '  %s is in the default tree\n' "$crate"
		fail=1
	fi
done

echo "default build, no openssl"
for crate in openssl openssl-sys native-tls; do
	if hits=$(printf '%s\n' "$tree" | grep -E "^${crate} v" || true); [ -n "$hits" ]; then
		printf '  %s is in the default tree\n' "$crate"
		fail=1
	fi
done

# The other direction, and the reason the feature exists. If turning `emulation`
# on ever stops pulling BoringSSL in, the profile is not doing what doc 05.5
# says it does and the JA4 self check is measuring rustls.
echo "emulation build, boringssl present"
emu=$(cargo tree -p umi-fetch --features emulation --edges normal --prefix none --format '{p}')
# Counted rather than `grep -q`, because `grep -q` stops reading at the first
# match, the printf upstream takes SIGPIPE, and `set -o pipefail` then reports
# the pipeline as failed even though the thing we were looking for was there.
if [ "$(printf '%s\n' "$emu" | grep -cE '^btls v' || true)" -eq 0 ]; then
	printf '  the emulation build has no btls, so T2 is not BoringSSL\n'
	fail=1
fi

echo "emulation build, still no openssl"
for crate in openssl openssl-sys native-tls; do
	if hits=$(printf '%s\n' "$emu" | grep -E "^${crate} v" || true); [ -n "$hits" ]; then
		printf '  %s is in the emulation tree, which cannot link with boringssl\n' "$crate"
		fail=1
	fi
done

# T3 brings chromiumoxide, which brings reqwest, and a reqwest that arrived with
# a TLS feature turned on would put a second stack in the tree without anybody
# meaning to. The crate only wants one for its `fetcher` feature, which
# downloads a Chromium build and which we leave off, so this is the check that
# the reason we left it off is still the reason it is off.
echo "render build, no second tls stack"
render=$(cargo tree -p umi-fetch --features render --edges normal --prefix none --format '{p}')
for crate in openssl openssl-sys native-tls btls btls-sys boring boring-sys; do
	if hits=$(printf '%s\n' "$render" | grep -E "^${crate} v" || true); [ -n "$hits" ]; then
		printf '  %s is in the render tree, which should be rustls only\n' "$crate"
		fail=1
	fi
done

# Both features at once is what the fleet runs, so it is what has to link. T2
# needs BoringSSL and T3 must not add anything to it.
echo "emulation and render together, boringssl and nothing else"
both=$(cargo tree -p umi-fetch --features emulation,render --edges normal --prefix none --format '{p}')
if [ "$(printf '%s\n' "$both" | grep -cE '^btls v' || true)" -eq 0 ]; then
	printf '  the fleet build has no btls, so T2 is not BoringSSL\n'
	fail=1
fi
for crate in openssl openssl-sys native-tls; do
	if hits=$(printf '%s\n' "$both" | grep -E "^${crate} v" || true); [ -n "$hits" ]; then
		printf '  %s is in the fleet tree, which cannot link with boringssl\n' "$crate"
		fail=1
	fi
done

if [ "$fail" -ne 0 ]; then
	echo
	echo "gate 2.2 failed, see docs/spec/05-fetch-tiers.md section 5.5"
	exit 1
fi

echo "ok"
