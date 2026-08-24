.PHONY: all check fmt lint test doc deny spec build release clean

# What CI runs, in the order CI runs it, so that a red build is
# reproducible on a laptop without reading a yaml file.
all: check

check: fmt lint test doc spec

fmt:
	cargo fmt --all --check

lint:
	cargo clippy --workspace --all-targets --all-features -- -D warnings

test:
	cargo test --workspace --all-features

doc:
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps

deny:
	cargo deny check advisories bans licenses sources

spec:
	./scripts/check-spec.sh

build:
	cargo build --workspace

release:
	cargo build --release --locked --bin umi --bin umid

fix:
	cargo fmt --all
	cargo clippy --workspace --all-targets --all-features --fix --allow-dirty

clean:
	cargo clean
