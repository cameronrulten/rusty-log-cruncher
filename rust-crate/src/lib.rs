//! Core library: implements the rollup in Rust-Polars and exposes it to Python.
let val = col(value);

// Calculate rolling mean/std, then a simple z-score and boolean flag.
let lf = df.lazy().with_columns([
val.clone()
.rolling_mean(opts.clone())
.over(over_exprs.clone())
.alias("roll_mean"),
val.clone()
.rolling_std(opts.clone())
.over(over_exprs.clone())
.alias("roll_std"),
])
.with_columns([
// z = (x - mean) / std ; guard against zero std to avoid NaNs/Infs
when(col("roll_std").gt(lit(0.0)))
.then((val.clone() - col("roll_mean")) / col("roll_std"))
.otherwise(lit(0.0))
.alias("zscore"),
])
.with_columns([
col("zscore").abs().gt(lit(z_thr)).alias("is_anomaly"),
]);

let out = lf.collect()?;
Ok(out)
}

// ----------------- Python bindings -----------------

/// Python-facing function that accepts/returns **Polars** DataFrames to keep
/// the boundary zero-copy via Arrow buffers.
#[pyfunction]
fn rollup_polars(df: PyDataFrame, keys: Vec<String>, value: String, window: usize, z_thr: f64) -> PyResult<PyDataFrame> {
let rust_df = df.into_df();
match rollup_impl(rust_df, &keys, &value, window, z_thr) {
Ok(df) => Ok(PyDataFrame(df)),
Err(e) => Err(pyo3::exceptions::PyValueError::new_err(e.to_string())),
}
}

/// Expose the module as `rusty_cruncher._rusty_cruncher` in Python.
#[pymodule]
fn _rusty_cruncher(_py: Python, m: &PyModule) -> PyResult<()> {
m.add_function(wrap_pyfunction!(rollup_polars, m)?)?;
Ok(())
}

// ----------------- Reusable CLI helpers -----------------

/// Read one or more CSV/JSON files in parallel and vertically concatenate.
pub fn read_logs_parallel(paths: &[String]) -> Result<DataFrame> {
use rayon::prelude::*;
let mut dfs: Vec<DataFrame> = paths
.par_iter()
.map(|p| -> Result<DataFrame> {
if p.ends_with(".json") || p.ends_with(".jsonl") {
let df = JsonReader::from_path(p)?.infer_schema_len(Some(10_000)).finish()?;
Ok(df)
} else {
// CSV as default; allow large files (streaming via Polars reader).
let df = CsvReadOptions::default()
.with_has_header(true)
.with_infer_schema_length(Some(10_000))
.try_into_reader_with_file_path(Some(p.into()))?
.finish()?;
Ok(df)
}
})
.collect::<Result<Vec<_>>>()?;

// Concatenate eagerly (streaming concat is also available in Polars if needed).
let df = polars::functions::concat_df(&mut dfs)?;
Ok(df)
}

/// Public entry for CLI: compute rollup on files and return a DataFrame.
pub fn rollup_on_files(paths: &[String], keys: &[String], value: &str, window: usize, z_thr: f64) -> Result<DataFrame> {
let df = read_logs_parallel(paths)?;
rollup_impl(df, keys, value, window, z_thr)
}