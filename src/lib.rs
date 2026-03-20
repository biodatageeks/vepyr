use datafusion::prelude::SessionContext;
use datafusion_bio_function_vep::register_vep_functions;
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

/// Register VEP annotation functions into a polars-bio SessionContext.
///
/// Called via `polars_bio.ctx.register_extension(vepyr._core._register_vep)`.
/// Receives a raw pointer to `SessionContext` plus the DataFusion version
/// string for ABI compatibility verification.
#[pyfunction]
fn _register_vep(ctx_ptr: usize, datafusion_version: &str) -> PyResult<()> {
    let our_version = datafusion::DATAFUSION_VERSION;
    if datafusion_version != our_version {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
            "DataFusion version mismatch: polars-bio has {datafusion_version}, \
             vepyr has {our_version}. Both must use the same version."
        )));
    }
    let ctx = unsafe { &*(ctx_ptr as *const SessionContext) };
    register_vep_functions(ctx);
    Ok(())
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(convert_entity, m)?)?;
    m.add_function(wrap_pyfunction!(_register_vep, m)?)?;
    Ok(())
}
