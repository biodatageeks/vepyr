use datafusion::prelude::SessionContext;
use datafusion_bio_function_vep::register_vep_functions;
use pyo3::prelude::*;

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

/// Register VEP annotation functions into a polars-bio SessionContext.
///
/// Called via `polars_bio.ctx.register_extension(vepyr._core._register_vep)`.
///
/// # Safety
///
/// The `ctx_ptr` is a raw pointer to a `datafusion::prelude::SessionContext`.
/// Both polars-bio and vepyr MUST be built with the same DataFusion version
/// and Rust compiler to ensure ABI compatibility. The version checks below
/// verify this at runtime.
#[pyfunction]
fn _register_vep(ctx_ptr: usize, datafusion_version: &str) -> PyResult<()> {
    if datafusion_version != DATAFUSION_VERSION {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
            "DataFusion version mismatch: polars-bio={datafusion_version}, \
             vepyr={DATAFUSION_VERSION}. Both must be built with the same version."
        )));
    }
    // SAFETY: caller (polars-bio register_extension) guarantees the pointer
    // is valid for the duration of this call, and we verified the DataFusion
    // version matches. Both crates must be built from the same CI/toolchain.
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
    m.add_function(wrap_pyfunction!(_register_vep, m)?)?;
    m.add_function(wrap_pyfunction!(_version_info, m)?)?;
    Ok(())
}
