use pyo3::prelude::*;

mod convert;

/// Convert a single entity type from an Ensembl VEP cache to Parquet.
///
/// Progress is printed to stderr (visible in both terminal and Jupyter).
///
/// Args:
///     cache_root: Path to the unpacked Ensembl VEP cache directory (contains info.txt).
///     output_dir: Directory where Parquet files will be written.
///     entity: Entity type: variation, transcript, exon, translation, regulatory, motif.
///     partitions: Number of DataFusion partitions for parallelism (default: 8).
///
/// Returns:
///     List of (file_path, row_count) tuples, or None if no source files exist.
#[pyfunction]
#[pyo3(signature = (cache_root, output_dir, entity, partitions=8))]
fn convert_entity(
    cache_root: &str,
    output_dir: &str,
    entity: &str,
    partitions: usize,
) -> PyResult<Option<Vec<(String, usize)>>> {
    match convert::convert_entity(cache_root, output_dir, entity, partitions) {
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
