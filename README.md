# Rusty Log Cruncher

**Goal**: Fast, safe log rollups in Rust (Polars) exposed to Python, with benchmarks.

## Quickstart (macOS)

```bash
# One-time prerequisites
xcode-select --install # Apple Command Line Tools
brew install uv # Astral's fast Python manager
curl https://sh.rustup.rs -sSf | sh # Rust toolchain

# Create/refresh the Python project env
cd python
uv sync
uv run -- python -V # Sanity check

# Build the Rust extension and install into this env
uv run -- maturin develop -m ../rust-crate/Cargo.toml

# Smoke test in Python
uv run -- python - <<'PY'
import polars as pl
from rusty_cruncher import rollup

df = pl.DataFrame({"service":["A","A","B","B"], "value":[1.0,2.0,1.5,9.0]})
print(rollup(df, keys=["service"], value="value", window=2))
PY

