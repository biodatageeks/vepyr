use datafusion_bio_format_ensembl_cache::CacheSourceType;
use datafusion_bio_function_vep::cache_builder::{CacheBuilder, OnProgress};
use pyo3::prelude::*;

mod annotate;
mod plugin_convert;
mod plugin_fjall;

fn parse_cache_source_type(value: &str) -> PyResult<CacheSourceType> {
    value.parse::<CacheSourceType>().map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "Invalid cache_source_type '{value}': {err}"
        ))
    })
}

/// Build all entities from an Ensembl VEP cache to parquet + optional fjall.
///
/// Returns a list of `(entity, [(parquet_path, rows)], Option<(variants, positions, bytes, secs)>)`.
#[pyfunction]
#[pyo3(signature = (cache_root, output_dir, partitions=8, build_fjall=true, zstd_level=3, dict_size_kb=112, on_progress=None, cache_source_type="ensembl"))]
#[allow(clippy::type_complexity, clippy::too_many_arguments)]
fn build_cache(
    py: Python<'_>,
    cache_root: &str,
    output_dir: &str,
    partitions: usize,
    build_fjall: bool,
    zstd_level: i32,
    dict_size_kb: u32,
    on_progress: Option<PyObject>,
    cache_source_type: &str,
) -> PyResult<Vec<(String, Vec<(String, usize)>, Option<(u64, u64, u64, f64)>)>> {
    let cache_source_type = parse_cache_source_type(cache_source_type)?;

    let cb: Option<OnProgress> = on_progress.map(|py_cb| {
        Box::new(
            move |entity: &str, fmt: &str, batch: usize, total: usize, expected: usize| {
                Python::with_gil(|py| {
                    if let Err(e) = py_cb.call1(py, (entity, fmt, batch, total, expected)) {
                        log::warn!("on_progress callback error: {e}");
                    }
                });
            },
        ) as OnProgress
    });

    let mut builder = CacheBuilder::new(cache_root, output_dir)
        .with_partitions(partitions)
        .with_build_fjall(build_fjall)
        .with_zstd_level(zstd_level)
        .with_dict_size_kb(dict_size_kb)
        .with_cache_source_type(cache_source_type);

    if let Some(progress) = cb {
        builder = builder.with_on_progress(progress);
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(partitions)
        .enable_all()
        .build()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{e}")))?;

    let stats = py.allow_threads(|| {
        rt.block_on(builder.build_all()).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Cache build failed: {e}"))
        })
    })?;

    let result: Vec<(String, Vec<(String, usize)>, Option<(u64, u64, u64, f64)>)> = stats
        .into_iter()
        .map(|s| {
            let fjall = s.fjall_stats.map(|f| {
                (
                    f.total_variants,
                    f.total_positions,
                    f.total_bytes,
                    f.elapsed_secs,
                )
            });
            (s.entity, s.parquet_files, fjall)
        })
        .collect();

    Ok(result)
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

/// Convert a VEP plugin source file (VCF.gz or TSV.gz) to per-chromosome Parquet.
#[pyfunction]
#[pyo3(signature = (plugin_name, source_path, output_dir, partitions=8, memory_limit_gb=32, chromosomes=None, assume_sorted_input=false, preview_rows=None))]
#[allow(clippy::too_many_arguments)]
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

/// Convert CADD SNV + indel TSV sources to per-chromosome Parquet.
#[pyfunction]
#[pyo3(signature = (snv_source_path, indel_source_path, output_dir, partitions=8, memory_limit_gb=32, chromosomes=None, assume_sorted_input=false, preview_rows=None))]
#[allow(clippy::too_many_arguments)]
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

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = env_logger::try_init();
    m.add_class::<annotate::StreamingAnnotator>()?;
    m.add_function(wrap_pyfunction!(build_cache, m)?)?;
    m.add_function(wrap_pyfunction!(create_annotator, m)?)?;
    m.add_function(wrap_pyfunction!(annotate_vcf, m)?)?;
    m.add_function(wrap_pyfunction!(convert_plugin, m)?)?;
    m.add_function(wrap_pyfunction!(convert_cadd_plugin, m)?)?;
    m.add_function(wrap_pyfunction!(build_plugin_fjall, m)?)?;
    Ok(())
}
