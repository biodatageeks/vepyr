use datafusion_bio_format_ensembl_cache::CacheSourceType;
use datafusion_bio_function_vep::cache_builder::{CacheBuilder, OnProgress};
use datafusion_bio_function_vep::warm_cache::build::{
    build_warm_variation_tier, WarmVariationTierOptions,
};
use pyo3::prelude::*;

mod annotate;

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

/// Rebuild the warm/cold variation cache tier for one or more chromosomes.
///
/// Returns `(chrom, warm_positions, warm_rows, cold_rows, warm_row_groups,
/// cold_row_groups, cold_rows_sharing_warm_positions, row_group_position_splits)`.
#[pyfunction]
#[pyo3(signature = (cache_dir, chroms=None, af_threshold=0.01, position_radius=1, row_group_rows=500_000, batch_size=65_536))]
#[allow(clippy::type_complexity, clippy::too_many_arguments)]
fn build_variation_cache_tier(
    cache_dir: &str,
    chroms: Option<Vec<String>>,
    af_threshold: f64,
    position_radius: i64,
    row_group_rows: usize,
    batch_size: usize,
) -> PyResult<Vec<(String, usize, usize, usize, usize, usize, usize, usize)>> {
    let cache_root = std::path::Path::new(cache_dir);
    let variation_dir = cache_root.join("variation");
    if !variation_dir.is_dir() {
        return Err(pyo3::exceptions::PyFileNotFoundError::new_err(format!(
            "variation directory not found: {}",
            variation_dir.display()
        )));
    }

    let chroms = match chroms {
        Some(chroms) => chroms,
        None => discover_variation_chroms(&variation_dir)?,
    };
    if chroms.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "no variation parquet files found in {}",
            variation_dir.display()
        )));
    }

    let mut out = Vec::with_capacity(chroms.len());
    for chrom in chroms {
        let input = variation_dir.join(format!("{chrom}.parquet"));
        if !input.is_file() {
            return Err(pyo3::exceptions::PyFileNotFoundError::new_err(format!(
                "variation parquet not found: {}",
                input.display()
            )));
        }
        let mut options = WarmVariationTierOptions::new(input, variation_dir.clone());
        options.af_threshold = af_threshold;
        options.position_radius = position_radius;
        options.row_group_rows = row_group_rows;
        options.batch_size = batch_size;
        let stats = build_warm_variation_tier(options).map_err(|err| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "variation cache tier build failed for {chrom}: {err}"
            ))
        })?;
        out.push((
            stats.chrom,
            stats.warm_positions,
            stats.warm_rows,
            stats.cold_rows,
            stats.warm_row_groups,
            stats.cold_row_groups,
            stats.cold_rows_sharing_warm_positions,
            stats.row_group_position_splits,
        ));
    }

    Ok(out)
}

fn discover_variation_chroms(variation_dir: &std::path::Path) -> PyResult<Vec<String>> {
    let mut chroms = Vec::new();
    for entry in std::fs::read_dir(variation_dir).map_err(|err| {
        pyo3::exceptions::PyRuntimeError::new_err(format!(
            "failed to read {}: {err}",
            variation_dir.display()
        ))
    })? {
        let path = entry
            .map_err(|err| pyo3::exceptions::PyRuntimeError::new_err(format!("{err}")))?
            .path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("parquet") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        if stem.ends_with("_warm") || stem.ends_with("_cold") {
            continue;
        }
        chroms.push(stem.to_string());
    }
    chroms.sort_by_key(|chrom| chrom_sort_key(chrom));
    Ok(chroms)
}

fn chrom_sort_key(chrom: &str) -> (u8, u32, String) {
    let normalized = chrom.strip_prefix("chr").unwrap_or(chrom);
    match normalized {
        "X" => (0, 23, normalized.to_string()),
        "Y" => (0, 24, normalized.to_string()),
        "MT" | "M" => (0, 25, normalized.to_string()),
        _ => normalized
            .parse::<u32>()
            .map(|n| (0, n, normalized.to_string()))
            .unwrap_or_else(|_| (1, u32::MAX, normalized.to_string())),
    }
}

/// Annotate a VCF and write results directly to a VCF file.
/// Returns the number of rows written.
#[pyfunction]
#[pyo3(signature = (vcf_path, cache_dir, output_path, options_json, show_progress=true, compression="", on_batch_written=None, forks=0))]
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
    forks: usize,
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
        forks,
    )
}

/// Create a streaming VEP annotator that yields PyArrow RecordBatches.
#[pyfunction]
#[pyo3(signature = (vcf_path, cache_dir, options_json, skip_csq=true, limit=None, forks=0))]
#[allow(clippy::too_many_arguments)]
fn create_annotator(
    py: Python<'_>,
    vcf_path: &str,
    cache_dir: &str,
    options_json: &str,
    skip_csq: bool,
    limit: Option<usize>,
    forks: usize,
) -> PyResult<annotate::StreamingAnnotator> {
    annotate::create_streaming_annotator(
        py,
        vcf_path,
        cache_dir,
        options_json,
        skip_csq,
        limit,
        forks,
    )
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = env_logger::try_init();
    m.add_class::<annotate::StreamingAnnotator>()?;
    m.add_function(wrap_pyfunction!(build_cache, m)?)?;
    m.add_function(wrap_pyfunction!(build_variation_cache_tier, m)?)?;
    m.add_function(wrap_pyfunction!(create_annotator, m)?)?;
    m.add_function(wrap_pyfunction!(annotate_vcf, m)?)?;
    Ok(())
}
