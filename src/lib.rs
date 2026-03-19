use pyo3::prelude::*;

mod convert;

/// Convert a single entity type from an Ensembl VEP cache to Parquet.
#[pyfunction]
#[pyo3(signature = (cache_root, output_dir, entity, partitions=8, memory_limit_gb=32))]
fn convert_entity(
    cache_root: &str,
    output_dir: &str,
    entity: &str,
    partitions: usize,
    memory_limit_gb: usize,
) -> PyResult<Option<Vec<(String, usize)>>> {
    match convert::convert_entity(cache_root, output_dir, entity, partitions, memory_limit_gb) {
        Ok(results) => Ok(Some(results)),
        Err(e) if e == "skipped" => Ok(None),
        Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e)),
    }
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(convert_entity, m)?)?;
    Ok(())
}
