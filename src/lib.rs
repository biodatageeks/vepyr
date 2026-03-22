use arrow::pyarrow::ToPyArrow;
use datafusion::prelude::SessionContext;
use datafusion_bio_function_vep::register_vep_functions;
use pyo3::prelude::*;

mod annotate;
mod convert;

/// DataFusion version for ABI compatibility check.
const DATAFUSION_VERSION: &str = datafusion::DATAFUSION_VERSION;

/// Rustc version baked at compile time for ABI compatibility check.
const RUSTC_VERSION: &str = env!("RUSTC_VERSION");

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

/// Run VEP annotation on a VCF file using a standalone DataFusion session.
///
/// This bypasses polars-bio's session entirely, creating its own DataFusion
/// context with VEP functions registered. Returns a PyArrow Table.
#[pyfunction]
#[pyo3(signature = (vcf_path, cache_dir, options_json))]
fn run_annotate(
    py: Python<'_>,
    vcf_path: &str,
    cache_dir: &str,
    options_json: &str,
) -> PyResult<PyObject> {
    let batches = annotate::run_annotate(vcf_path, cache_dir, options_json)
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;

    // Convert RecordBatches to PyArrow Table
    if batches.is_empty() {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(
            "Annotation produced no results",
        ));
    }

    let schema = batches[0].schema();
    let py_batches: Vec<PyObject> = batches
        .iter()
        .map(|batch| batch.to_pyarrow(py))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{e}")))?;

    let py_schema = schema.to_pyarrow(py)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{e}")))?;
    let pa = py.import("pyarrow")?;
    let table = pa
        .getattr("Table")?
        .call_method1("from_batches", (py_batches, py_schema))?;

    Ok(table.into())
}

/// Register VEP annotation functions into a polars-bio SessionContext.
#[pyfunction]
fn _register_vep(ctx_ptr: usize, datafusion_version: &str) -> PyResult<()> {
    if datafusion_version != DATAFUSION_VERSION {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
            "DataFusion version mismatch: polars-bio={datafusion_version}, \
             vepyr={DATAFUSION_VERSION}. Both must be built with the same version."
        )));
    }
    let ctx = unsafe { &*(ctx_ptr as *const SessionContext) };
    register_vep_functions(ctx);
    Ok(())
}

/// Return version info for compatibility diagnostics.
#[pyfunction]
fn _version_info() -> (String, String) {
    (DATAFUSION_VERSION.to_string(), RUSTC_VERSION.to_string())
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(convert_entity, m)?)?;
    m.add_function(wrap_pyfunction!(run_annotate, m)?)?;
    m.add_function(wrap_pyfunction!(_register_vep, m)?)?;
    m.add_function(wrap_pyfunction!(_version_info, m)?)?;
    Ok(())
}
