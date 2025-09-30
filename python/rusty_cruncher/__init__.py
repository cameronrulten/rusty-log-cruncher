"""Python-friendly wrappers around the Rust extension.

We accept either a pandas.DataFrame or a polars.DataFrame, convert to Polars
for zero-copy into Rust, and then return a DataFrame in the same frontend as
was provided (pandas in => pandas out; Polars in => Polars out).
"""
from __future__ import annotations
from typing import Iterable, Sequence

import pandas as pd
import polars as pl

# The Rust extension compiled by PyO3/maturin
from ._rusty_cruncher import rollup_polars as _rollup_polars


def _to_polars(df) -> pl.DataFrame:
if isinstance(df, pl.DataFrame):
return df
if isinstance(df, pd.DataFrame):
return pl.from_pandas(df)
# Try last-ditch constructor (works for Arrow Tables, dicts of lists, etc.)
return pl.DataFrame(df)


def rollup(
df,
*,
keys: Sequence[str],
value: str,
window: int = 128,
z: float = 3.0,
return_backend: str | None = None,
):
"""Compute rolling mean/std + z-score anomaly flag per group.

Parameters
----------
df : pandas.DataFrame | polars.DataFrame | Arrow-like
Input tabular data.
keys : sequence[str]
Group-by columns (partition keys).
value : str
Numeric column to analyze.
window : int, default 128
Row-based rolling window length.
z : float, default 3.0
Absolute z-score threshold for `is_anomaly`.
return_backend : {"input", "polars", "pandas"} or None
Controls the return type. If None or "input", returns the same frontend
as the input (pandas in => pandas out). Use "polars"/"pandas" to force.
"""
was_pandas = isinstance(df, pd.DataFrame)
df_pl = _to_polars(df)
out_pl = _rollup_polars(df_pl, list(keys), str(value), int(window), float(z))

choice = return_backend or ("pandas" if was_pandas else "polars")
if choice == "pandas":
return out_pl.to_pandas()
return out_pl