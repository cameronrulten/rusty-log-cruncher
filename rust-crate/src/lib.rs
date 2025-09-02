use pyo3::prelude::*;

#[pyfunction]
fn rollup(_py: Python<'_>) -> PyResult<String> {
    Ok("ok".to_string())
}

#[pymodule]
fn rusty_log_cruncher(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(rollup, m)?)?;
    Ok(())
}
