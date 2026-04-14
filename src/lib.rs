use pyo3::prelude::*;

mod annotate;
mod core_fjall;
mod convert;
mod plugin_convert;
mod plugin_fjall;

/// Convert a single entity type from an Ensembl VEP cache to Parquet.
#[pyfunction]
#[pyo3(signature = (cache_root, output_dir, entity, partitions=8, memory_limit_gb=32, chromosomes=None))]
fn convert_entity(
    cache_root: &str,
    output_dir: &str,
    entity: &str,
    partitions: usize,
    memory_limit_gb: usize,
    chromosomes: Option<Vec<String>>,
) -> PyResult<Option<Vec<(String, usize)>>> {
    match convert::convert_entity(
        cache_root,
        output_dir,
        entity,
        partitions,
        memory_limit_gb,
        chromosomes,
    ) {
        Ok(results) => Ok(Some(results)),
        Err(e) if e == "skipped" => Ok(None),
        Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e)),
    }
}

/// Annotate a VCF and write results directly to a VCF file.
/// Returns the number of rows written.
#[pyfunction]
#[pyo3(signature = (vcf_path, cache_dir, output_path, options_json, show_progress=true, compression="", on_batch_written=None))]
#[allow(clippy::too_many_arguments)]
fn annotate_vcf(
    py: Python<'_>,
    vcf_path: &str,
    cache_dir: &str,
    output_path: &str,
    options_json: &str,
    show_progress: bool,
    compression: &str,
    on_batch_written: Option<PyObject>,
) -> PyResult<usize> {
    annotate::annotate_to_vcf_file(
        py,
        vcf_path,
        cache_dir,
        output_path,
        options_json,
        show_progress,
        compression,
        on_batch_written,
    )
}

/// Create a streaming VEP annotator that yields PyArrow RecordBatches.
#[pyfunction]
#[pyo3(signature = (vcf_path, cache_dir, options_json, skip_csq=true, limit=None))]
fn create_annotator(
    py: Python<'_>,
    vcf_path: &str,
    cache_dir: &str,
    options_json: &str,
    skip_csq: bool,
    limit: Option<usize>,
) -> PyResult<annotate::StreamingAnnotator> {
    annotate::create_streaming_annotator(py, vcf_path, cache_dir, options_json, skip_csq, limit)
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<annotate::StreamingAnnotator>()?;
    m.add_function(wrap_pyfunction!(convert_entity, m)?)?;
    m.add_function(wrap_pyfunction!(create_annotator, m)?)?;
    m.add_function(wrap_pyfunction!(annotate_vcf, m)?)?;
    m.add_function(wrap_pyfunction!(convert_plugin, m)?)?;
    m.add_function(wrap_pyfunction!(convert_cadd_plugin, m)?)?;
    m.add_function(wrap_pyfunction!(build_plugin_fjall, m)?)?;
    m.add_function(wrap_pyfunction!(build_entity_fjall, m)?)?;
    Ok(())
}

/// Convert a VEP plugin source file (VCF.gz or TSV.gz) to per-chromosome Parquet.
#[pyfunction]
#[pyo3(signature = (plugin_name, source_path, output_dir, partitions=8, memory_limit_gb=32, chromosomes=None, assume_sorted_input=false, preview_rows=None))]
fn convert_plugin(
    plugin_name: &str,
    source_path: &str,
    output_dir: &str,
    partitions: usize,
    memory_limit_gb: usize,
    chromosomes: Option<Vec<String>>,
    assume_sorted_input: bool,
    preview_rows: Option<usize>,
) -> PyResult<Vec<(String, usize)>> {
    plugin_convert::convert_plugin(
        plugin_name,
        source_path,
        output_dir,
        partitions,
        memory_limit_gb,
        chromosomes,
        assume_sorted_input,
        preview_rows,
    )
    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
}

#[pyfunction]
#[pyo3(signature = (snv_source_path, indel_source_path, output_dir, partitions=8, memory_limit_gb=32, chromosomes=None, assume_sorted_input=false, preview_rows=None))]
fn convert_cadd_plugin(
    snv_source_path: &str,
    indel_source_path: &str,
    output_dir: &str,
    partitions: usize,
    memory_limit_gb: usize,
    chromosomes: Option<Vec<String>>,
    assume_sorted_input: bool,
    preview_rows: Option<usize>,
) -> PyResult<Vec<(String, usize)>> {
    plugin_convert::convert_cadd_plugin(
        snv_source_path,
        indel_source_path,
        output_dir,
        partitions,
        memory_limit_gb,
        chromosomes,
        assume_sorted_input,
        preview_rows,
    )
    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
}

/// Convert plugin Parquet files into a fjall point-lookup cache.
#[pyfunction]
#[pyo3(signature = (plugin_name, parquet_dir, output_path, partitions=8, chromosomes=None))]
fn build_plugin_fjall(
    plugin_name: &str,
    parquet_dir: &str,
    output_path: &str,
    partitions: usize,
    chromosomes: Option<Vec<String>>,
) -> PyResult<(String, usize)> {
    plugin_fjall::build_plugin_fjall(
        plugin_name,
        parquet_dir,
        output_path,
        partitions,
        chromosomes,
    )
    .map_err(pyo3::exceptions::PyRuntimeError::new_err)
}

/// Build a core fjall cache from existing parquet files for one entity.
#[pyfunction]
#[pyo3(signature = (cache_root, output_dir, entity, partitions=8, chromosomes=None))]
fn build_entity_fjall(
    cache_root: &str,
    output_dir: &str,
    entity: &str,
    partitions: usize,
    chromosomes: Option<Vec<String>>,
) -> PyResult<Vec<(String, usize)>> {
    core_fjall::build_entity_fjall(cache_root, output_dir, entity, partitions, chromosomes)
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)
}
