use pyo3::prelude::*;

mod convert;

/// Convert an Ensembl VEP cache directory to optimized Parquet files.
///
/// Converts all entity types (variation, transcript, exon, translation,
/// regulatory features, motif features) from the raw Ensembl cache format
/// to deduplicated, sorted Parquet files with ZSTD compression.
///
/// Translation is split into two files: translation_core (sorted by transcript_id)
/// and translation_sift (sorted by chrom, start).
///
/// Args:
///     cache_root: Path to the unpacked Ensembl VEP cache directory (contains info.txt).
///     output_dir: Directory where Parquet files will be written.
///     partitions: Number of DataFusion partitions for parallelism (default: 8).
///
/// Returns:
///     List of (file_path, row_count) tuples for all written files.
#[pyfunction]
#[pyo3(signature = (cache_root, output_dir, partitions=8))]
fn cache_to_parquet(
    cache_root: &str,
    output_dir: &str,
    partitions: usize,
) -> PyResult<Vec<(String, usize)>> {
    convert::cache_to_parquet(cache_root, output_dir, partitions)
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(cache_to_parquet, m)?)?;
    Ok(())
}
