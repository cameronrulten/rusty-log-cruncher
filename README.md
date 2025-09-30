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

# --- Prerequisites ---
# Debian/Ubuntu
sudo apt update
sudo apt install -y build-essential pkg-config python3 python3-venv python3-dev curl git

# (Fedora/RHEL)
# sudo dnf groupinstall -y "Development Tools"
# sudo dnf install -y python3-devel pkgconf-pkg-config curl git

# Install uv (fast Python env/packaging manager)
curl -LsSf https://astral.sh/uv/install.sh | sh
exec "$SHELL" -l # reload PATH so `uv` is available

# Install Rust toolchain (for compiling the extension)
curl https://sh.rustup.rs -sSf | sh -s -- -y
source "$HOME/.cargo/env"

# --- Build & test ---
cd python
uv sync
uv run -- python -V

# Build the Rust extension and install into this env
uv run -- maturin develop -m ../rust-crate/Cargo.toml

# Smoke test in Python
uv run -- python - <<'PY'
import polars as pl
from rusty_cruncher import rollup

df = pl.DataFrame({"service":["A","A","B","B"], "value":[1.0,2.0,1.5,9.0]})
print(rollup(df, keys=["service"], value="value", window=2))
PY