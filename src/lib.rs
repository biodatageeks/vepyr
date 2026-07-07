use datafusion_bio_format_ensembl_cache::CacheSourceType;
use datafusion_bio_function_vep::cache_builder::{CacheBuilder, CacheFormat};
use datafusion_bio_function_vep::plugin_cache::builder::PluginCacheBuilder;
use datafusion_bio_function_vep::plugin_cache::source_manifest::SourceManifest;
use pyo3::prelude::*;
use pyo3::types::PyAny;

// The VEP consequence engine is allocation-heavy (CSQ strings, feature clones,
// HashMaps); macOS libmalloc is slow per-alloc and contends across threads,
// which capped within-contig parallel scaling. mimalloc fixes both — ~1.67x
// faster single-threaded and materially better thread scaling. A cdylib CAN
// set the global allocator (a library crate cannot), so it belongs here.
//
// Selection (exactly one is linked): dhat-heap (profiling) overrides everything;
// otherwise jemalloc when enabled (except Windows/MSVC), else mimalloc (default).
// Build jemalloc: maturin build --no-default-features --features extension-module,jemalloc
#[cfg(all(
    not(feature = "dhat-heap"),
    feature = "mimalloc",
    not(all(feature = "jemalloc", not(target_env = "msvc")))
))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(all(
    not(feature = "dhat-heap"),
    feature = "jemalloc",
    not(target_env = "msvc")
))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static GLOBAL: dhat::Alloc = dhat::Alloc;

mod annotate;

fn parse_cache_source_type(value: &str) -> PyResult<CacheSourceType> {
    value.parse::<CacheSourceType>().map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "Invalid cache_source_type '{value}': {err}"
        ))
    })
}

/// Build all entities from an Ensembl VEP cache.
///
/// Returns a list of `(entity, [(parquet_path, rows)], Option<(variants, positions, bytes, secs)>)`.
#[pyfunction]
#[pyo3(signature = (cache_root, output_dir, partitions=8, cache_format="parquet", on_progress=None, cache_source_type="ensembl", overwrite=false))]
#[allow(clippy::type_complexity, clippy::too_many_arguments)]
fn build_cache(
    py: Python<'_>,
    cache_root: &str,
    output_dir: &str,
    partitions: usize,
    cache_format: &str,
    on_progress: Option<Py<PyAny>>,
    cache_source_type: &str,
    overwrite: bool,
) -> PyResult<Vec<(String, Vec<(String, usize)>, Option<(u64, u64, u64, f64)>)>> {
    let cache_source_type = parse_cache_source_type(cache_source_type)?;
    let cache_format = CacheFormat::parse(cache_format).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!("Invalid cache_format: {err}"))
    })?;

    // The Parquet build path does not invoke a progress callback; the parameter
    // is retained only for backward-compatible Python API.
    let _ = on_progress;

    let builder = CacheBuilder::new(cache_root, output_dir)
        .with_partitions(partitions)
        .with_cache_format(cache_format)
        .with_cache_source_type(cache_source_type)
        .with_overwrite(overwrite);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(partitions)
        .enable_all()
        .build()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{e}")))?;

    // Release the GIL so tokio worker threads can run in parallel.
    // The progress callback re-acquires it via Python::with_gil() when needed.
    let stats = py.detach(|| {
        rt.block_on(builder.build_all()).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Cache build failed: {e}"))
        })
    })?;

    // Convert EntityStats to Python-friendly tuples
    let result: Vec<(String, Vec<(String, usize)>, Option<(u64, u64, u64, f64)>)> = stats
        .into_iter()
        .map(|s| {
            // The legacy fjall backend has been removed; the dependency no
            // longer carries an `fjall_stats` field. Keep the tuple shape for
            // the backward-compatible Python API (always `None`).
            let fjall: Option<(u64, u64, u64, f64)> = None;
            (s.entity, s.parquet_files, fjall)
        })
        .collect();

    Ok(result)
}

/// Build a plugin cache (all chroms, or a filtered set) from a source manifest.
/// Returns per-chrom `(chrom, rows, warm, cold)` tuples.
#[pyfunction]
#[pyo3(signature = (manifest_path, source_path, variation_cache_dir, plugin_cache_root, chroms=None, overwrite=false))]
fn build_plugin_cache(
    py: Python<'_>,
    manifest_path: &str,
    source_path: &str,
    variation_cache_dir: &str,
    plugin_cache_root: &str,
    chroms: Option<Vec<String>>,
    overwrite: bool,
) -> PyResult<Vec<(String, usize, usize, usize)>> {
    let mut manifest = SourceManifest::load(std::path::Path::new(manifest_path))
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("load manifest: {e}")))?;
    // The public API takes a single `source_path`, so it can only override one
    // source. A multi-part manifest (multiple `[[source]]` blocks) would leave
    // later sources on their stale placeholder paths — fail fast instead of
    // silently reading the wrong file.
    if manifest.sources.len() > 1 {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "manifest declares {} [[source]] entries; build_plugin_cache takes a single \
             source_path and cannot map multi-part sources (not yet supported)",
            manifest.sources.len()
        )));
    }
    if let Some(first) = manifest.sources.first_mut() {
        first.path = source_path.to_string();
    }
    // The builder always rewrites each chrom shard (its `with_overwrite` is a
    // no-op in v0.14.0), so guard here against an accidental FULL rebuild. Only an
    // UNFILTERED build (`chroms=None`) rewrites every chrom and clobbers the whole
    // cache — refuse that without `overwrite`. A filtered build (`chroms=[...]`)
    // is an explicit, targeted request that upserts into the existing manifest, so
    // it's allowed (enables incremental per-chromosome builds).
    if !overwrite && chroms.is_none() {
        let out_manifest = std::path::Path::new(plugin_cache_root)
            .join("plugin")
            .join(&manifest.plugin_name)
            .join("manifest.json");
        if out_manifest.exists() {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "plugin cache already exists at {} (pass overwrite=True to rebuild all \
                 chromosomes, or chroms=[...] to add/rebuild specific ones)",
                out_manifest.display()
            )));
        }
    }
    let manifest_file = std::path::Path::new(manifest_path)
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| manifest_path.to_string());

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{e}")))?;

    let cache = py.detach(|| {
        rt.block_on(async {
            let mut b = PluginCacheBuilder::new(
                &manifest,
                &manifest_file,
                variation_cache_dir,
                plugin_cache_root,
            )
            .with_overwrite(overwrite);
            if let Some(cs) = chroms {
                b = b.with_chrom_filter(cs);
            }
            b.build_all().await
        })
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("plugin build failed: {e}")))
    })?;

    Ok(cache
        .chroms
        .into_iter()
        .map(|c| (c.chrom, c.rows, c.warm, c.cold))
        .collect())
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
    on_batch_written: Option<Py<PyAny>>,
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
    m.add_function(wrap_pyfunction!(build_plugin_cache, m)?)?;
    m.add_function(wrap_pyfunction!(create_annotator, m)?)?;
    m.add_function(wrap_pyfunction!(annotate_vcf, m)?)?;
    Ok(())
}
