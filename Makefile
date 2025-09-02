.PHONY: build wheel bench

build:
	cd rust-crate && maturin develop

wheel:
	cd rust-crate && maturin build --release

bench:
	cd rust-crate && cargo bench
