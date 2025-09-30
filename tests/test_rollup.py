import numpy as np
import pandas as pd
import polars as pl
import pytest

from rusty_cruncher import rollup


def test_smoke_pandas():
n = 1000
df = pd.DataFrame({
"service": np.where(np.arange(n) % 2 == 0, "A", "B"),
"value": np.linspace(0, 10, n),
})
out = rollup(df, keys=["service"], value="value", window=32)
assert {"roll_mean", "roll_std", "zscore", "is_anomaly"}.issubset(out.columns)


@pytest.mark.benchmark(group="rollup")
def test_benchmark_polars(benchmark):
n = 500_000
df = pl.DataFrame({
"service": ["A" if i % 2 == 0 else "B" for i in range(n)],
"value": np.sin(np.arange(n)) + 0.1 * np.arange(n),
})
def _run():
return rollup(df, keys=["service"], value="value", window=128, return_backend="polars")
result = benchmark(_run)
assert result.height == n