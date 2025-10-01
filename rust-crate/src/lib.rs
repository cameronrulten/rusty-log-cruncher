//! Core library: implements rollups in Rust-Polars and exposes them to Python.
//! Python module name compiled by PyO3 is `_rusty_cruncher`.

use anyhow::{anyhow, Result};
use polars::prelude::*;
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame; // thin wrapper for Polars <-> Python

// ----------------- core implementation -----------------

/// Do the heavy lifting in pure Rust/Polars.
///
/// * `df`      — input DataFrame (eager).
/// * `keys`    — partition-by columns (group keys).
/// * `value`   — numeric column to compute stats over.
/// * `window`  — row-based rolling window size (e.g., 128 => last 128 rows per group).
/// * `z_thr`   — anomaly threshold on absolute z-score.
pub(crate) fn rollup_impl(
    df: DataFrame,
    keys: &[String],
    value: &str,
    window: usize,
    z_thr: f64,
) -> Result<DataFrame> {
    if keys.is_empty() {
        return Err(anyhow!("keys must not be empty"));
    }
    if !df.get_column_names().iter().any(|c| c == value) {
        return Err(anyhow!("value column '{value}' not found"));
    }

    // Rolling window options (row-based).
    let opts = RollingOptionsFixedWindow {
        window_size: window,
        min_periods: 1,
        weights: None,
        center: false,
        fn_params: None,
    };

    let over_exprs: Vec<Expr> = keys.iter().map(|k| col(k)).collect();
    let val = col(value);

    // Compute rolling mean/std per group, then z-score and anomaly flag.
    let lf = df
        .lazy()
        .with_columns([
            val.clone()
                .rolling_mean(opts.clone())
                .over(over_exprs.clone())
                .alias("roll_mean"),
            val.clone()
                .rolling_std(opts.clone())
                .over(over_exprs.clone())
                .alias("roll_std"),
        ])
        .with_columns([when(col("roll_std").gt(lit(0.0)))
            .then((val.clone() - col("roll_mean")) / col("roll_std"))
            .otherwise(lit(0.0))
            .alias("zscore")])
        .with_columns([col("zscore").abs().gt(lit(z_thr)).alias("is_anomaly")]);

    Ok(lf.collect()?)
}

// ----------------- Python bindings -----------------

/// Accept/return **Polars** DataFrames to keep the boundary zero-copy via Arrow.
#[pyfunction]
fn rollup_polars(
    df: PyDataFrame,
    keys: Vec<String>,
    value: String,
    window: usize,
    z_thr: f64,
) -> PyResult<PyDataFrame> {
    let rust_df = df.into_df();
    match rollup_impl(rust_df, &keys, &value, window, z_thr) {
        Ok(df) => Ok(PyDataFrame(df)),
        Err(e) => Err(pyo3::exceptions::PyValueError::new_err(e.to_string())),
    }
}

/// Expose as `rusty_cruncher._rusty_cruncher` in Python (see pyproject module-name).
#[pymodule]
fn _rusty_cruncher(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(rollup_polars, m)?)?;
    Ok(())
}

// ----------------- CLI helpers (pure Rust) -----------------

/// Read one or more CSV/JSON files (simple, eager) and vertically concatenate.
pub fn read_logs_parallel(paths: &[String]) -> Result<DataFrame> {
    use rayon::prelude::*;

    let mut dfs: Vec<DataFrame> = paths
        .par_iter()
        .map(|p| -> Result<DataFrame> {
            if p.ends_with(".json") || p.ends_with(".jsonl") {
                // JSON reader
                Ok(JsonReader::from_path(p)?
                    .infer_schema_len(Some(10_000))
                    .finish()?)
            } else {
                // CSV reader
                Ok(CsvReader::from_path(p)?
                    .has_header(true)
                    .infer_schema(Some(10_000))
                    .finish()?)
            }
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(polars::functions::concat_df(&mut dfs)?)
}

/// Public entry for CLI: compute rollup on files and return a DataFrame.
pub fn rollup_on_files(
    paths: &[String],
    keys: &[String],
    value: &str,
    window: usize,
    z_thr: f64,
) -> Result<DataFrame> {
    let df = read_logs_parallel(paths)?;
    rollup_impl(df, keys, value, window, z_thr)
}
