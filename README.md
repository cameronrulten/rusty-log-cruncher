# Rusty Log Cruncher

High-performance rollups (group-by, window stats) with Rust + Polars, exposed to Python via PyO3/maturin.

## Quickstart (macOS/Linux)
```bash
# Rust toolchain
curl https://sh.rustup.rs -sSf | sh
pip install maturin
make build
python -c "import rusty_log_cruncher as r; print(r.rollup())"
