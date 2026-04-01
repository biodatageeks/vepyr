use datafusion_bio_function_vep::cache_builder::{CacheBuilder, OnProgress};
use pyo3::prelude::*;

mod annotate;

/// Build all entities from an Ensembl VEP cache to parquet + optional fjall.
///
/// Returns a list of `(entity, [(parquet_path, rows)], Option<(variants, positions, bytes, secs)>)`.
#[pyfunction]
#[pyo3(signature = (cache_root, output_dir, partitions=8, build_fjall=true, zstd_level=3, dict_size_kb=112, on_progress=None))]
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
) -> PyResult<Vec<(String, Vec<(String, usize)>, Option<(u64, u64, u64, f64)>)>> {
    let cb: Option<OnProgress> = on_progress.map(|py_cb| {
        Box::new(
            move |entity: &str, fmt: &str, batch: usize, total: usize, expected: usize| {
                Python::with_gil(|py| {
                    let _ = py_cb.call1(py, (entity, fmt, batch, total, expected));
                });
            },
        ) as OnProgress
    });

    let mut builder = CacheBuilder::new(cache_root, output_dir)
        .with_partitions(partitions)
        .with_build_fjall(build_fjall)
        .with_zstd_level(zstd_level)
        .with_dict_size_kb(dict_size_kb);

    if let Some(progress) = cb {
        builder = builder.with_on_progress(progress);
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(partitions)
        .enable_all()
        .build()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{e}")))?;

    // Release the GIL so tokio worker threads can run in parallel.
    // The progress callback re-acquires it via Python::with_gil() when needed.
    let stats = py.allow_threads(|| {
        rt.block_on(builder.build_all()).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Cache build failed: {e}"))
        })
    })?;

    // Convert EntityStats to Python-friendly tuples
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

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    pyo3_log::init();
    m.add_class::<annotate::StreamingAnnotator>()?;
    m.add_function(wrap_pyfunction!(build_cache, m)?)?;
    m.add_function(wrap_pyfunction!(create_annotator, m)?)?;
    m.add_function(wrap_pyfunction!(annotate_vcf, m)?)?;
    Ok(())
}
