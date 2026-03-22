use datafusion::prelude::SessionContext;
use datafusion_bio_function_vep::register_vep_functions;
use pyo3::prelude::*;

mod annotate;
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

/// Create a streaming VEP annotator that yields PyArrow RecordBatches.
///
/// Returns a StreamingAnnotator iterator. Each call to __next__ yields
/// one PyArrow RecordBatch. Use with polars register_io_source for
/// true streaming LazyFrame support.
#[pyfunction]
#[pyo3(signature = (vcf_path, cache_dir, options_json))]
fn create_annotator(
    py: Python<'_>,
    vcf_path: &str,
    cache_dir: &str,
    options_json: &str,
) -> PyResult<annotate::StreamingAnnotator> {
    annotate::create_streaming_annotator(py, vcf_path, cache_dir, options_json)
}

/// Register VEP functions into a polars-bio SessionContext (kept for future use).
#[pyfunction]
fn _register_vep(ctx_ptr: usize, datafusion_version: &str) -> PyResult<()> {
    let our_version = datafusion::DATAFUSION_VERSION;
    if datafusion_version != our_version {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
            "DataFusion version mismatch: polars-bio={datafusion_version}, vepyr={our_version}"
        )));
    }
    let ctx = unsafe { &*(ctx_ptr as *const SessionContext) };
    register_vep_functions(ctx);
    Ok(())
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<annotate::StreamingAnnotator>()?;
    m.add_function(wrap_pyfunction!(convert_entity, m)?)?;
    m.add_function(wrap_pyfunction!(create_annotator, m)?)?;
    m.add_function(wrap_pyfunction!(_register_vep, m)?)?;
    Ok(())
}
