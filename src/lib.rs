use datafusion_bio_function_vep::cache_builder::{CacheBuilder, EntityStats, OnProgress};
use datafusion_bio_function_vep::kv_cache::{CacheBackend, LoadStats};
use pyo3::prelude::*;

mod annotate;

/// Build all entities from an Ensembl VEP cache to parquet + optional fjall.
///
/// Returns a list of `(entity, [(parquet_path, rows)], Option<(variants, positions, bytes, secs)>)`.
#[pyfunction]
#[pyo3(signature = (cache_root, output_dir, partitions=8, build_fjall=true, zstd_level=3, dict_size_kb=112, on_progress=None, kv_backend=None, compact_redb=false))]
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
    kv_backend: Option<String>,
    compact_redb: bool,
) -> PyResult<Vec<(String, Vec<(String, usize)>, Option<(u64, u64, u64, f64)>)>> {
    let selected_backend = match kv_backend.as_deref() {
        Some("none") => None,
        Some("fjall") => Some(CacheBackend::Fjall),
        Some("redb") => Some(CacheBackend::Redb),
        Some(other) => {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "kv_backend must be one of 'none', 'fjall', or 'redb', got {other:?}"
            )));
        }
        None if build_fjall => Some(CacheBackend::Fjall),
        None => None,
    };

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
        .with_build_fjall(matches!(selected_backend, Some(CacheBackend::Fjall)))
        .with_zstd_level(zstd_level)
        .with_dict_size_kb(dict_size_kb)
        .with_compact_redb_after_load(compact_redb);

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
        rt.block_on(async {
            let mut stats = builder.build_all().await.map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("Cache build failed: {e}"))
            })?;

            if matches!(selected_backend, Some(CacheBackend::Redb)) {
                let redb_entities =
                    builder
                        .build_variation_redb_from_parquet()
                        .await
                        .map_err(|e| {
                            pyo3::exceptions::PyRuntimeError::new_err(format!(
                                "redb cache build failed: {e}"
                            ))
                        })?;
                let redb_stats = redb_entities
                    .into_iter()
                    .find(|entity| entity.entity == "variation")
                    .and_then(|entity| entity.fjall_stats);
                if let Some(redb_stats) = redb_stats {
                    attach_entity_stats(&mut stats, "variation", redb_stats);
                }

                let redb_sift_entities =
                    builder.build_sift_redb_from_parquet().await.map_err(|e| {
                        pyo3::exceptions::PyRuntimeError::new_err(format!(
                            "redb sift cache build failed: {e}"
                        ))
                    })?;
                let redb_sift_stats = redb_sift_entities
                    .into_iter()
                    .find(|entity| entity.entity == "translation_sift")
                    .and_then(|entity| entity.fjall_stats);
                if let Some(redb_sift_stats) = redb_sift_stats {
                    attach_entity_stats(&mut stats, "translation_sift", redb_sift_stats);
                }
            }

            Ok::<_, PyErr>(stats)
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

fn attach_entity_stats(stats: &mut Vec<EntityStats>, entity_name: &str, load_stats: LoadStats) {
    if let Some(entity) = stats.iter_mut().find(|s| s.entity == entity_name) {
        entity.fjall_stats = Some(load_stats);
    } else {
        stats.push(EntityStats {
            entity: entity_name.to_string(),
            parquet_files: Vec::new(),
            fjall_stats: Some(load_stats),
        });
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
    let _ = env_logger::try_init();
    m.add_class::<annotate::StreamingAnnotator>()?;
    m.add_function(wrap_pyfunction!(build_cache, m)?)?;
    m.add_function(wrap_pyfunction!(create_annotator, m)?)?;
    m.add_function(wrap_pyfunction!(annotate_vcf, m)?)?;
    Ok(())
}
