.PHONY: dev build clean test bench fmt lint cli

# Compile and install the extension into the project venv (via uv run)
dev:
cd python && uv run -- maturin develop -m ../rust-crate/Cargo.toml

# Build wheels into target/wheels
build:
cd python && uv run -- maturin build -m ../rust-crate/Cargo.toml --release

clean:
cargo clean && rm -rf python/.venv target

test:
cd python && uv run -- pytest -q

bench:
cargo bench && cd python && uv run -- pytest -q -k benchmark --benchmark-only

fmt:
cargo fmt && cargo clippy --all-targets -- -D warnings

cli:
cargo run --bin rusty-cruncher -- --help