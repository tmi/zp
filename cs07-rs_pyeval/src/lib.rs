use pyo3::prelude::*;
use std::collections::HashMap;
use std::collections::HashSet;

#[pyfunction]
fn resolve_expression(raw: String, variables: HashMap<String, String>) -> PyResult<String> {
    pyeval_core::resolve_expression(&raw, &variables)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))
}

#[pyfunction]
fn extract_glyphs(raw: String) -> PyResult<HashSet<String>> {
    pyeval_core::extract_glyphs(&raw)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))
}

#[pymodule]
fn _rs_pyeval(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(resolve_expression, m)?)?;
    m.add_function(wrap_pyfunction!(extract_glyphs, m)?)?;
    Ok(())
}
