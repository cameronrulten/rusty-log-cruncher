//! Core library: rollups in Rust-Polars with Python bindings via PyO3.
//! Compiled Python module name: `_rusty_cruncher`.

use anyhow::{anyhow, Result};
use polars::prelude::*;
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;

// ----------------- core implementation -----------------

/// Compute rolling mean/std, z-score, and anomaly flag within groups.
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
    // get_column_names() returns &PlSmallStr; compare by deref
    if !df.get_column_names().iter().any(|c| *c == value) {
        return Err(anyhow!("value column '{value}' not found"));
    }

    let opts = RollingOptionsFixedWindow {
        window_size: window,
        min_periods: 1,
        weights: None,
        center: false,
        fn_params: None,
    };

    let over_exprs: Vec<Expr> = keys.iter().map(|k| col(k)).collect();
    let val = col(value);

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

#[pyfunction]
fn rollup_polars(
    df: PyDataFrame,
    keys: Vec<String>,
    value: String,
    window: usize,
    z_thr: f64,
) -> PyResult<PyDataFrame> {
    // pyo3-polars uses Into to get the Rust DataFrame
    let rust_df: DataFrame = df.into();
    match rollup_impl(rust_df, &keys, &value, window, z_thr) {
        Ok(df) => Ok(PyDataFrame(df)),
        Err(e) => Err(pyo3::exceptions::PyValueError::new_err(e.to_string())),
    }
}

#[pymodule]
fn _rusty_cruncher(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(rollup_polars, m)?)?;
    Ok(())
}

// ----------------- CLI helpers (pure Rust) -----------------

/// Read one or more CSV files and vertically concatenate.
/// (JSON can be added later; right now we keep IO surface minimal & stable.)
pub fn read_logs_parallel(paths: &[String]) -> Result<DataFrame> {
    use rayon::prelude::*;

    let mut dfs: Vec<DataFrame> = paths
        .par_iter()
        .map(|p| -> Result<DataFrame> {
            // Basic heuristic: treat everything as CSV for now.
            // (Add JSON reader once you want it; needs extra feature gates.)
            let df = CsvReadOptions::default()
                .with_has_header(true)
                .with_infer_schema_length(Some(10_000))
                .try_into_reader_with_file_path(Some(p.into()))?
                .finish()?;
            Ok(df)
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(polars::functions::concat_df(&mut dfs)?)
}

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
