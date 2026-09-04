use datafusion_bio_format_ensembl_cache::CacheSourceType;
use datafusion_bio_function_vep::cache_builder::{CacheBuilder, CacheFormat};
use datafusion_bio_function_vep::plugin_cache::builder::PluginCacheBuilder;
use datafusion_bio_function_vep::plugin_cache::source_manifest::SourceManifest;
use datafusion_bio_function_vep::plugin_cache::source_verify::SourceVerification;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use std::path::Path;

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

/// Return the compiled Ensembl VEP/cache support matrix as JSON.
///
/// JSON keeps this ABI stable and lets the Python package expose immutable
/// copies without maintaining a second hand-written compatibility table.
#[pyfunction]
fn supported_vep_targets_json() -> PyResult<String> {
    let targets = datafusion_bio_function_vep::vep_semantics::supported_vep_targets()
        .iter()
        .map(|target| {
            serde_json::json!({
                "vepyr_version": env!("CARGO_PKG_VERSION"),
                "cache_version": target.cache_version,
                "vep_codebase_version": target.vep_codebase_version,
                "api_version": target.api_version,
                "ensembl_core_revision": target.ensembl_core_revision,
                "ensembl_variation_revision": target.ensembl_variation_revision,
                "semantics": target.semantics.as_str(),
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_string(&targets).map_err(|error| {
        pyo3::exceptions::PyRuntimeError::new_err(format!(
            "failed to serialize supported VEP targets: {error}"
        ))
    })
}

/// Validate and return the embedded identity for one cache contig as JSON.
#[pyfunction]
#[pyo3(signature = (cache_dir, chrom, expected_cache_version=None))]
fn cache_contig_identity_json(
    py: Python<'_>,
    cache_dir: &str,
    chrom: &str,
    expected_cache_version: Option<String>,
) -> PyResult<String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| pyo3::exceptions::PyRuntimeError::new_err(error.to_string()))?;
    let identity = py
        .detach(|| {
            rt.block_on(
                datafusion_bio_function_vep::cache_identity::validate_partitioned_cache_contig(
                    cache_dir,
                    chrom,
                    expected_cache_version,
                ),
            )
        })
        .map_err(|error| pyo3::exceptions::PyRuntimeError::new_err(error.to_string()))?;
    serde_json::to_string(&serde_json::json!({
        "vepyr_version": env!("CARGO_PKG_VERSION"),
        "cache_source_type": identity.cache_source_type,
        "cache_version": identity.cache_version,
        "vep_codebase_version": identity.target.vep_codebase_version,
        "api_version": identity.target.api_version,
        "ensembl_core_revision": identity.target.ensembl_core_revision,
        "ensembl_variation_revision": identity.target.ensembl_variation_revision,
        "semantics": identity.target.semantics.as_str(),
        "contig": chrom,
    }))
    .map_err(|error| {
        pyo3::exceptions::PyRuntimeError::new_err(format!(
            "failed to serialize cache identity: {error}"
        ))
    })
}

/// Build all entities from an Ensembl VEP cache.
///
/// Returns a list of `(entity, [(parquet_path, rows)], Option<(variants, positions, bytes, secs)>)`.
#[pyfunction]
#[pyo3(signature = (cache_root, output_dir, partitions=8, cache_format="parquet", on_progress=None, cache_source_type="ensembl", overwrite=false, expected_cache_version=None))]
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
    expected_cache_version: Option<String>,
) -> PyResult<Vec<(String, Vec<(String, usize)>, Option<(u64, u64, u64, f64)>)>> {
    let cache_source_type = parse_cache_source_type(cache_source_type)?;
    let cache_format = CacheFormat::parse(cache_format).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!("Invalid cache_format: {err}"))
    })?;

    // The Parquet build path does not invoke a progress callback; the parameter
    // is retained only for backward-compatible Python API.
    let _ = on_progress;

    let mut builder = CacheBuilder::new(cache_root, output_dir)
        .with_partitions(partitions)
        .with_cache_format(cache_format)
        .with_cache_source_type(cache_source_type)
        .with_overwrite(overwrite);
    if let Some(expected_cache_version) = expected_cache_version {
        builder = builder.with_expected_cache_version(expected_cache_version);
    }

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

/// Build a single entity from an Ensembl VEP cache, leaving the rest of the
/// cache directory untouched.
///
/// A schema change usually affects one entity, and rebuilding the whole cache
/// to pick it up costs an hour and tens of gigabytes. `entity` is one of
/// "variation", "transcript", "exon", "translation", "regulatory", "motif".
///
/// Returns the same `(entity, [(parquet_path, rows)], None)` shape as
/// [`build_cache`].
#[pyfunction]
#[pyo3(signature = (cache_root, output_dir, entity, partitions=8, cache_source_type="ensembl", overwrite=true, expected_cache_version=None, chroms=None))]
#[allow(clippy::type_complexity, clippy::too_many_arguments)]
fn build_cache_entity(
    py: Python<'_>,
    cache_root: &str,
    output_dir: &str,
    entity: &str,
    partitions: usize,
    cache_source_type: &str,
    overwrite: bool,
    expected_cache_version: Option<String>,
    chroms: Option<Vec<String>>,
) -> PyResult<Vec<(String, Vec<(String, usize)>, Option<(u64, u64, u64, f64)>)>> {
    let cache_source_type = parse_cache_source_type(cache_source_type)?;

    let mut builder = CacheBuilder::new(cache_root, output_dir)
        .with_partitions(partitions)
        .with_cache_format(CacheFormat::Parquet)
        .with_cache_source_type(cache_source_type)
        .with_overwrite(overwrite);
    if let Some(expected_cache_version) = expected_cache_version {
        builder = builder.with_expected_cache_version(expected_cache_version);
    }
    // Restrict the rebuild to specific contigs. An empty list means no filter,
    // matching `CacheBuilder::with_chrom_filter`.
    if let Some(chroms) = chroms {
        builder = builder.with_chrom_filter(chroms);
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(partitions)
        .enable_all()
        .build()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{e}")))?;

    let stats = py.detach(|| {
        rt.block_on(builder.build_entity(entity)).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Cache build failed: {e}"))
        })
    })?;

    Ok(stats
        .into_iter()
        .map(|s| (s.entity, s.parquet_files, None))
        .collect())
}

/// Build a plugin cache (all chroms, or a filtered set) from a source manifest.
/// Returns per-chrom `(chrom, rows, warm, cold)` tuples.
/// Point each `[[source]]` in the manifest at a real file.
///
/// `source_path` is either a single path (for a one-source manifest) or a
/// `{part: path}` mapping. A manifest's shipped `path` values are placeholders,
/// so anything left unmapped would silently read the wrong file — every source
/// must be assigned exactly once, and the mapping must not name a part the
/// manifest does not declare.
fn apply_source_paths(
    manifest: &mut SourceManifest,
    source_path: &Bound<'_, PyAny>,
) -> PyResult<()> {
    let err = pyo3::exceptions::PyValueError::new_err;

    if let Ok(single) = source_path.extract::<String>() {
        if manifest.sources.len() > 1 {
            let parts: Vec<&str> = manifest
                .sources
                .iter()
                .map(|s| s.part.as_deref().unwrap_or("<no part>"))
                .collect();
            return Err(err(format!(
                "manifest declares {} [[source]] entries ({}); pass a dict mapping each \
                 part to its path, e.g. source_path={{{}}}",
                manifest.sources.len(),
                parts.join(", "),
                parts
                    .iter()
                    .map(|p| format!("{p:?}: \"...\""))
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
        if let Some(only) = manifest.sources.first_mut() {
            only.path = single;
        }
        return Ok(());
    }

    let mut mapping: std::collections::HashMap<String, String> = source_path
        .extract()
        .map_err(|_| err("source_path must be a str or a dict of {part: path}".to_string()))?;

    for source in manifest.sources.iter_mut() {
        let part = source.part.clone().ok_or_else(|| {
            err(format!(
                "source_path was given as a dict, but a [[source]] in plugin {:?} declares \
                 no `part` to key it by",
                manifest.plugin_name
            ))
        })?;
        let path = mapping.remove(&part).ok_or_else(|| {
            err(format!(
                "source_path is missing an entry for part {part:?}; every [[source]] must \
                 be mapped or it would read its placeholder path"
            ))
        })?;
        source.path = path;
    }
    if !mapping.is_empty() {
        let mut unknown: Vec<String> = mapping.into_keys().collect();
        unknown.sort();
        let declared: Vec<&str> = manifest
            .sources
            .iter()
            .filter_map(|s| s.part.as_deref())
            .collect();
        return Err(err(format!(
            "source_path names part(s) {unknown:?} that the manifest does not declare \
             (it declares {declared:?})"
        )));
    }
    Ok(())
}

/// Make the staged cache at `staged_dir` the live one at `plugin_dir`, by
/// renames only, so no step can leave the cache half removed: the live cache
/// is set aside at `previous`, the staged one renamed into place, and the
/// caller deletes `previous` afterwards. Another full overwrite of the same
/// plugin may finish in between — the live directory can vanish before the
/// first rename or reappear before the second — so both renames are retried:
/// a missing live cache means there is nothing to set aside, and a live cache
/// that reappeared is another writer's complete cache, which is set aside in
/// turn so the later writer wins. A rename that fails for any other reason
/// puts the previous cache back and reports where everything is.
fn install_overwrite(
    staged_dir: &Path,
    plugin_dir: &Path,
    previous_base: &Path,
    leftovers: &mut Vec<String>,
) -> PyResult<Option<std::path::PathBuf>> {
    let io = |what: String, e: std::io::Error| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("overwrite: {what}: {e}"))
    };
    if let Some(parent) = plugin_dir.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| io(format!("failed to create {}", parent.display()), e))?;
    }
    const ATTEMPTS: usize = 8;
    for attempt in 1..=ATTEMPTS {
        // A fresh set-aside path per attempt: a superseded set-aside tree that
        // could not be removed must not make the next rename fail.
        let previous = if attempt == 1 {
            previous_base.to_path_buf()
        } else {
            std::path::PathBuf::from(format!("{}-{attempt}", previous_base.display()))
        };
        let previous = previous.as_path();
        let had_previous = match std::fs::rename(plugin_dir, previous) {
            Ok(()) => true,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
            Err(e) => {
                return Err(io(
                    format!(
                        "failed to set the previous cache aside at {}",
                        previous.display()
                    ),
                    e,
                ));
            }
        };
        match std::fs::rename(staged_dir, plugin_dir) {
            Ok(()) => return Ok(had_previous.then(|| previous.to_path_buf())),
            Err(e) if plugin_dir.exists() && attempt < ATTEMPTS => {
                // Another overwrite installed its complete cache between the
                // two renames. The cache we set aside is older than that one,
                // so it is dropped, and the newer live cache is set aside next.
                if had_previous {
                    leftovers.extend(remove_leftovers(&[(
                        previous,
                        "a superseded previous cache",
                    )]));
                }
                log::info!(
                    "overwrite: {} was replaced by another writer during the swap ({e}); \
                     retrying (attempt {attempt} of {ATTEMPTS})",
                    plugin_dir.display()
                );
            }
            Err(e) => {
                let mut what = format!(
                    "failed to move the new cache {} into place at {}",
                    staged_dir.display(),
                    plugin_dir.display()
                );
                if had_previous {
                    match std::fs::rename(previous, plugin_dir) {
                        Ok(()) => what.push_str("; the previous cache was put back"),
                        Err(restore) => what.push_str(&format!(
                            "; putting the previous cache back also failed ({restore}), it is \
                             intact at {} — rename it to {} to recover",
                            previous.display(),
                            plugin_dir.display()
                        )),
                    }
                }
                return Err(io(what, e));
            }
        }
    }
    Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
        "overwrite: gave up installing {} after {ATTEMPTS} attempts; another full overwrite \
         of the same plugin keeps replacing it",
        plugin_dir.display()
    )))
}

/// A full overwrite swaps the live cache out by two renames; a process killed
/// between them leaves the cache under `<root>/.previous-<plugin>-*` and
/// nothing at its path. The next call puts it back when there is exactly one
/// such tree and no live cache, so an interrupted swap is not a permanent loss.
fn recover_set_aside_cache(root: &Path, plugin_dir: &Path, plugin_name: &str) {
    if plugin_dir.exists() {
        return;
    }
    let prefix = format!(".previous-{plugin_name}-");
    let candidates: Vec<std::path::PathBuf> = std::fs::read_dir(root)
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(&prefix))
                && p.join("manifest.json").exists()
        })
        .collect();
    if let [only] = candidates.as_slice() {
        if let Some(parent) = plugin_dir.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        match std::fs::rename(only, plugin_dir) {
            Ok(()) => log::warn!(
                "recovered the plugin cache {} from {}, left behind by an interrupted overwrite",
                plugin_dir.display(),
                only.display()
            ),
            Err(e) => log::warn!(
                "could not recover {} from {}: {e}",
                plugin_dir.display(),
                only.display()
            ),
        }
    }
}

/// Remove directories that are no longer needed, returning a description of
/// each one that could not be removed but still exists.
fn remove_leftovers(dirs: &[(&Path, &str)]) -> Vec<String> {
    let mut leftovers = Vec::new();
    for (path, what) in dirs {
        if let Err(e) = std::fs::remove_dir_all(path) {
            if path.exists() {
                leftovers.push(format!("{what} at {} ({e})", path.display()));
            }
        }
    }
    leftovers
}

/// A cleanup that fails must not pass silently: a leftover staging or
/// set-aside tree is a plugin cache's worth of disk.
fn warn_leftovers(py: Python<'_>, leftovers: Vec<String>) -> PyResult<()> {
    if leftovers.is_empty() {
        return Ok(());
    }
    let message = std::ffi::CString::new(format!(
        "overwrite: {} could not be removed; delete it by hand to reclaim the space",
        leftovers.join(" and ")
    ))
    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{e}")))?;
    pyo3::PyErr::warn(
        py,
        &py.get_type::<pyo3::exceptions::PyRuntimeWarning>(),
        &message,
        1,
    )
}

#[pyfunction]
#[pyo3(signature = (manifest_path, source_path, variation_cache_dir, plugin_cache_root, chroms=None, overwrite=false, verify_source="strict"))]
#[allow(clippy::too_many_arguments, clippy::type_complexity)]
fn build_plugin_cache(
    py: Python<'_>,
    manifest_path: &str,
    source_path: &Bound<'_, PyAny>,
    variation_cache_dir: &str,
    plugin_cache_root: &str,
    chroms: Option<Vec<String>>,
    overwrite: bool,
    verify_source: &str,
) -> PyResult<(Vec<(String, usize, usize, usize)>, String)> {
    // Reject a bad mode before the (possibly expensive) manifest resolution
    // and before anything on disk is touched.
    let verification: SourceVerification = verify_source
        .parse()
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("verify_source: {e}")))?;
    let mut manifest = SourceManifest::load(std::path::Path::new(manifest_path))
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("load manifest: {e}")))?;
    apply_source_paths(&mut manifest, source_path)?;
    // The builder always rewrites each chrom shard (its `with_overwrite` is a
    // no-op in v0.14.0), so guard here against an accidental FULL rebuild. A build
    // is "full" when no chromosome filter narrows it — either `chroms=None` or an
    // empty list, which `PluginCacheBuilder::with_chrom_filter` also treats as no
    // filter (resolving/rebuilding every shard). Refuse that without `overwrite`.
    // A non-empty `chroms=[...]` is an explicit, targeted request that upserts into
    // the existing manifest, so it's allowed (enables incremental builds).
    let is_full_build = chroms.as_ref().is_none_or(|c| c.is_empty());
    let plugin_dir = std::path::Path::new(plugin_cache_root)
        .join("plugin")
        .join(&manifest.plugin_name);
    recover_set_aside_cache(
        Path::new(plugin_cache_root),
        &plugin_dir,
        &manifest.plugin_name,
    );
    // A full overwrite must end with only freshly built chromosomes: `build_all`
    // carries chromosomes of an earlier build over when their provenance
    // matches, and a smaller/different chrom set would otherwise leave stale
    // entries behind that `annotate()` keeps emitting. But the existing cache
    // must not be destroyed before the new one is known to be good — the
    // source is verified inside `build_all`, and a mismatch there would have
    // cost a multi-hour cache. So the replacement is built into a staging root
    // beside the cache and swapped into place only after the build succeeds;
    // on failure the staging root is removed and the old cache is untouched.
    // The staging root is unique to this invocation, so two overwrites of the
    // same plugin never build into, or delete, each other's tree; each swaps
    // in its own complete cache and the later one wins.
    let unique = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    let staging_root = if is_full_build {
        if overwrite {
            let staging = std::path::Path::new(plugin_cache_root)
                .join(format!(".overwrite-{}-{unique}", manifest.plugin_name));
            std::fs::create_dir_all(&staging).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "overwrite: failed to create staging directory {}: {e}",
                    staging.display()
                ))
            })?;
            Some(staging)
        } else if plugin_dir.join("manifest.json").exists() {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "plugin cache already exists at {} (pass overwrite=True to rebuild all \
                 chromosomes, or chroms=[...] to add/rebuild specific ones)",
                plugin_dir.join("manifest.json").display()
            )));
        } else {
            None
        }
    } else {
        None
    };
    let build_root: String = staging_root
        .as_ref()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|| plugin_cache_root.to_string());
    let manifest_file = std::path::Path::new(manifest_path)
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| manifest_path.to_string());

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{e}")))?;

    let built = py.detach(|| {
        rt.block_on(async {
            let mut b = PluginCacheBuilder::new(
                &manifest,
                &manifest_file,
                variation_cache_dir,
                build_root.as_str(),
            )
            .with_overwrite(overwrite)
            .with_source_verification(verification);
            if let Some(cs) = chroms {
                b = b.with_chrom_filter(cs);
            }
            b.build_all().await
        })
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("plugin build failed: {e}")))
    });
    let cache = match (built, &staging_root) {
        (Err(e), Some(staging)) => {
            // The build failed; the previous cache was never touched. A staging
            // tree that cannot be removed is reported alongside the error, since
            // it may hold a cache's worth of shards.
            warn_leftovers(py, remove_leftovers(&[(staging, "the staging directory")]))?;
            return Err(e);
        }
        (Err(e), None) => return Err(e),
        (Ok(cache), Some(staging)) => {
            let staged_dir = staging.join("plugin").join(&manifest.plugin_name);
            // Set aside beside the staging roots, never under `plugin/`: every
            // manifest-bearing directory there is discovered as a plugin, and a
            // set-aside tree that could not be removed would be loaded as one.
            let previous = Path::new(plugin_cache_root)
                .join(format!(".previous-{}-{unique}", manifest.plugin_name));
            let mut leftovers = Vec::new();
            let installed = install_overwrite(&staged_dir, &plugin_dir, &previous, &mut leftovers);
            leftovers.extend(remove_leftovers(&[(staging, "the staging directory")]));
            if let Ok(Some(set_aside)) = &installed {
                leftovers.extend(remove_leftovers(&[(set_aside, "the previous cache")]));
            }
            warn_leftovers(py, leftovers)?;
            installed?;
            cache
        }
        (Ok(cache), None) => cache,
    };

    // The provenance this build recorded, returned rather than read back from
    // the live manifest: another writer may replace the live cache at any time.
    let sources = serde_json::to_string(&cache.sources)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{e}")))?;
    Ok((
        cache
            .chroms
            .into_iter()
            .map(|c| (c.chrom, c.rows, c.warm, c.cold))
            .collect(),
        sources,
    ))
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
    m.add_function(wrap_pyfunction!(build_cache_entity, m)?)?;
    m.add_function(wrap_pyfunction!(build_plugin_cache, m)?)?;
    m.add_function(wrap_pyfunction!(create_annotator, m)?)?;
    m.add_function(wrap_pyfunction!(annotate_vcf, m)?)?;
    m.add_function(wrap_pyfunction!(supported_vep_targets_json, m)?)?;
    m.add_function(wrap_pyfunction!(cache_contig_identity_json, m)?)?;
    Ok(())
}
