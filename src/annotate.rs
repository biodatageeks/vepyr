use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::pyarrow::ToPyArrow;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_bio_format_vcf::VcfCompressionType;
use datafusion_bio_function_vep::register_vep_functions;
use datafusion_bio_function_vep::vcf_sink::{annotate_to_vcf, AnnotateVcfConfig, OnBatchWritten};
use futures::StreamExt;
use pyo3::prelude::*;
use serde_json::Value;
use tokio::runtime::{Builder, Runtime};

fn effective_session_partitions(use_fjall: bool, forks: usize) -> usize {
    if use_fjall && forks > 0 {
        forks
    } else {
        1
    }
}

fn effective_runtime_threads(use_fjall: bool, forks: usize) -> usize {
    if use_fjall && forks > 0 {
        forks
    } else {
        1
    }
}

fn runtime_for_parallelism(use_fjall: bool, forks: usize) -> PyResult<Arc<Runtime>> {
    // DataFusion uses `tokio::task::block_in_place()` while resolving table
    // metadata. That requires Tokio's multi-thread scheduler even when the VEP
    // annotation plan itself is strict single-lane (`forks=0`).
    let runtime = Builder::new_multi_thread()
        .worker_threads(effective_runtime_threads(use_fjall, forks))
        .build()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{e}")))?;

    Ok(Arc::new(runtime))
}

fn options_json_with_parallelism(options_json: &str, forks: usize) -> PyResult<(String, bool)> {
    let mut opts: Value = serde_json::from_str(options_json).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("Invalid options JSON: {e}"))
    })?;
    let object = opts
        .as_object_mut()
        .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("options JSON must be an object"))?;
    let use_fjall = object
        .get("use_fjall")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if forks > 0 && !use_fjall {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "forks > 0 requires use_fjall=True",
        ));
    }
    object.insert("forks".to_string(), Value::from(forks));
    object.insert("inline_lookup".to_string(), Value::from(forks == 0));
    let options_json = serde_json::to_string(&opts).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("Invalid options JSON: {e}"))
    })?;
    Ok((options_json, use_fjall))
}

#[derive(Debug)]
struct EnvGuard {
    previous: Vec<(&'static str, Option<OsString>)>,
}

impl EnvGuard {
    fn set(vars: Vec<(&'static str, OsString)>) -> Self {
        let previous = vars
            .iter()
            .map(|(key, _)| (*key, std::env::var_os(key)))
            .collect();
        for (key, value) in vars {
            // SAFETY: vepyr annotation is a blocking native call. The guard
            // restores process-global variables immediately after the stream
            // or VCF write finishes.
            unsafe { std::env::set_var(key, value) };
        }
        Self { previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (key, value) in self.previous.drain(..).rev() {
            if let Some(value) = value {
                // SAFETY: see EnvGuard::set.
                unsafe { std::env::set_var(key, value) };
            } else {
                // SAFETY: see EnvGuard::set.
                unsafe { std::env::remove_var(key) };
            }
        }
    }
}

fn string_option(opts: &Value, key: &str) -> Option<OsString> {
    opts.get(key)
        .and_then(|v| v.as_str())
        .map(|s| PathBuf::from(s).into_os_string())
}

fn warm_variation_env_guard(
    cache_dir: &str,
    opts: &Value,
    use_fjall: bool,
) -> PyResult<Option<EnvGuard>> {
    let enabled = opts
        .get("warm_variation_cache")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if !enabled {
        return Ok(None);
    }
    if !use_fjall {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "warm_variation_cache=True requires use_fjall=True",
        ));
    }

    let cache_root = Path::new(cache_dir);
    let variation_dir = string_option(opts, "warm_variation_dir")
        .unwrap_or_else(|| cache_root.join("variation").into_os_string());
    let cold_dir =
        string_option(opts, "variation_cold_dir").unwrap_or_else(|| variation_dir.clone());
    let position_index_dir = string_option(opts, "variation_position_index_dir")
        .unwrap_or_else(|| cache_root.join("variation.position_index").into_os_string());

    let mut vars = vec![
        ("VEP_WARM_VARIATION_CACHE", OsString::from("1")),
        ("VEP_WARM_VARIATION_DIR", variation_dir),
        ("VEP_VARIATION_COLD_DIR", cold_dir),
        ("VEP_VARIATION_POSITION_INDEX_DIR", position_index_dir),
    ];
    if let Some(batch_size) = opts
        .get("warm_variation_batch_size")
        .and_then(|v| v.as_u64())
        .filter(|n| *n > 0)
    {
        vars.push((
            "VEP_WARM_VARIATION_BATCH_SIZE",
            OsString::from(batch_size.to_string()),
        ));
    }

    Ok(Some(EnvGuard::set(vars)))
}

/// A streaming annotator that yields PyArrow RecordBatches.
/// Thread-safe: wraps the stream in a Mutex so polars can call from any thread.
#[pyclass]
pub struct StreamingAnnotator {
    rt: std::sync::Arc<Runtime>,
    stream: std::sync::Mutex<Option<SendableRecordBatchStream>>,
    _env_guard: Option<EnvGuard>,
    #[pyo3(get)]
    schema: PyObject,
}

#[pymethods]
impl StreamingAnnotator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        let mut guard = self.stream.lock().map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Lock poisoned: {e}"))
        })?;

        let stream = match guard.as_mut() {
            Some(s) => s,
            None => return Ok(None),
        };

        loop {
            let batch = self.rt.block_on(stream.next());
            match batch {
                Some(Ok(batch)) => {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    let py_batch = batch
                        .to_pyarrow(py)
                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{e}")))?;
                    return Ok(Some(py_batch));
                }
                Some(Err(e)) => {
                    return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "Annotation stream error: {e}"
                    )));
                }
                None => {
                    *guard = None;
                    return Ok(None);
                }
            }
        }
    }
}

/// Annotate a VCF and write results directly to a VCF file.
/// Returns the number of rows written.
///
/// `compression` is one of "bgzf", "gzip", "plain", or empty string for auto-detect from path.
/// `on_batch_written` is an optional Python callable invoked with
/// `(batch_rows, total_rows_written, total_input_rows)` after each batch is
/// written — designed for tqdm/Jupyter progress bars.
#[allow(clippy::too_many_arguments)]
pub fn annotate_to_vcf_file(
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
    log::info!(
        "annotate_to_vcf_file start: input={}, output={}, show_progress={}, compression={}",
        vcf_path,
        output_path,
        show_progress,
        compression
    );

    let (options_json, use_fjall) = options_json_with_parallelism(options_json, forks)?;
    let opts: Value = serde_json::from_str(&options_json).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("Invalid options JSON: {e}"))
    })?;
    let _warm_env_guard = warm_variation_env_guard(cache_dir, &opts, use_fjall)?;

    let backend = if use_fjall { "fjall" } else { "parquet" };
    let rt = runtime_for_parallelism(use_fjall, forks)?;

    let vcf_compression = match compression {
        "bgzf" => VcfCompressionType::Bgzf,
        "gzip" => VcfCompressionType::Gzip,
        "plain" => VcfCompressionType::Plain,
        _ => VcfCompressionType::from_path(output_path),
    };

    // Wrap Python callback in a Send+Sync closure for the Rust async world.
    // Callback signature: (batch_rows, total_rows_written, total_input_rows)
    let callback: Option<OnBatchWritten> = on_batch_written.map(|cb| -> OnBatchWritten {
        Box::new(
            move |batch_rows: usize, total_rows: usize, total_input: usize| {
                log::debug!(
                    "on_batch_written: batch_rows={}, total_rows={}, total_input={}",
                    batch_rows,
                    total_rows,
                    total_input
                );
                Python::with_gil(|py| {
                    if let Err(e) = cb.call1(py, (batch_rows, total_rows, total_input)) {
                        log::warn!("on_batch_written callback error: {e}");
                    }
                });
            },
        )
    });

    let config = AnnotateVcfConfig {
        everything: opts
            .get("everything")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        extended_probes: opts
            .get("extended_probes")
            .and_then(|v| v.as_bool())
            .unwrap_or(true),
        reference_fasta_path: opts
            .get("reference_fasta_path")
            .and_then(|v| v.as_str())
            .map(String::from),
        use_fjall,
        hgvs: opts.get("hgvs").and_then(|v| v.as_bool()).unwrap_or(false),
        hgvsc: opts.get("hgvsc").and_then(|v| v.as_bool()).unwrap_or(false),
        hgvsp: opts.get("hgvsp").and_then(|v| v.as_bool()).unwrap_or(false),
        shift_hgvs: opts.get("shift_hgvs").and_then(|v| v.as_bool()),
        no_escape: opts
            .get("no_escape")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        remove_hgvsp_version: opts
            .get("remove_hgvsp_version")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        hgvsp_use_prediction: opts
            .get("hgvsp_use_prediction")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        merged: false,
        refseq: false,
        gencode_basic: opts
            .get("gencode_basic")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        gencode_primary: opts
            .get("gencode_primary")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        all_refseq: opts
            .get("all_refseq")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        exclude_predicted: opts
            .get("exclude_predicted")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        pick: opts.get("pick").and_then(|v| v.as_bool()).unwrap_or(false),
        pick_allele: opts
            .get("pick_allele")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        per_gene: opts
            .get("per_gene")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        pick_allele_gene: opts
            .get("pick_allele_gene")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        flag_pick: opts
            .get("flag_pick")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        flag_pick_allele: opts
            .get("flag_pick_allele")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        flag_pick_allele_gene: opts
            .get("flag_pick_allele_gene")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        pick_order: opts
            .get("pick_order")
            .and_then(|v| v.as_str())
            .map(String::from),
        failed: opts.get("failed").and_then(|v| v.as_i64()),
        distance: opts.get("distance").and_then(|v| {
            v.as_str()
                .map(String::from)
                .or_else(|| v.as_i64().map(|n| n.to_string()))
        }),
        buffer_size: opts
            .get("buffer_size")
            .and_then(|v| v.as_u64())
            .and_then(|n| usize::try_from(n).ok())
            .filter(|n| *n > 0)
            .unwrap_or(datafusion_bio_function_vep::vcf_sink::VEP_DEFAULT_BUFFER_SIZE),
        forks: Some(forks),
        compression: vcf_compression,
        show_progress,
        on_batch_written: callback,
    };

    // Release the GIL so the Python background thread (in __init__.py) can
    // let Jupyter's main thread pump display updates for tqdm progress bars.
    // The on_batch_written callback re-acquires the GIL via Python::with_gil().
    py.allow_threads(|| {
        rt.block_on(async {
            let rows = annotate_to_vcf(vcf_path, cache_dir, backend, output_path, &config)
                .await
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!("VCF annotation failed: {e}"))
                })?;
            log::info!(
                "annotate_to_vcf_file complete: output={}, rows={}",
                output_path,
                rows
            );

            Ok(rows)
        })
    })
}

/// Create a streaming annotator that yields PyArrow RecordBatches.
#[allow(clippy::too_many_arguments)]
pub fn create_streaming_annotator(
    py: Python<'_>,
    vcf_path: &str,
    cache_dir: &str,
    options_json: &str,
    skip_csq: bool,
    limit: Option<usize>,
    forks: usize,
) -> PyResult<StreamingAnnotator> {
    let (options_json, use_fjall) = options_json_with_parallelism(options_json, forks)?;
    let opts: Value = serde_json::from_str(&options_json).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("Invalid options JSON: {e}"))
    })?;
    let env_guard = warm_variation_env_guard(cache_dir, &opts, use_fjall)?;
    let rt = runtime_for_parallelism(use_fjall, forks)?;

    let (stream, schema) = rt.block_on(async {
        let backend = if use_fjall { "fjall" } else { "parquet" };
        let session_partitions = effective_session_partitions(use_fjall, forks);

        let config = SessionConfig::new().with_target_partitions(session_partitions);
        let ctx = SessionContext::new_with_config(config);
        register_vep_functions(&ctx);

        let vcf_provider = datafusion_bio_format_vcf::table_provider::VcfTableProvider::new(
            vcf_path.to_string(),
            Some(vec![]),
            Some(vec![]),
            None,
            false,
        )
        .map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to open VCF: {e}"))
        })?;
        ctx.register_table("vcf", Arc::new(vcf_provider))
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to register VCF: {e}"))
            })?;

        let select = if skip_csq {
            "SELECT * EXCLUDE (\"CSQ\")"
        } else {
            "SELECT *"
        };

        let limit_clause = limit.map(|n| format!(" LIMIT {n}")).unwrap_or_default();
        let sql = format!(
            "{select} FROM annotate_vep('vcf', '{}', '{backend}', '{}'){limit_clause}",
            cache_dir.replace('\'', "''"),
            options_json.replace('\'', "''"),
        );

        let df = ctx
            .sql(&sql)
            .await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("SQL: {e}")))?;

        let schema = df.schema().inner().clone();
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Stream: {e}")))?;

        Ok::<_, PyErr>((stream, schema))
    })?;

    let py_schema = schema
        .to_pyarrow(py)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{e}")))?;

    Ok(StreamingAnnotator {
        rt,
        stream: std::sync::Mutex::new(Some(stream)),
        _env_guard: env_guard,
        schema: py_schema,
    })
}

#[cfg(test)]
mod tests {
    use super::{effective_runtime_threads, options_json_with_parallelism};

    #[test]
    fn forks_are_forwarded_as_chromosome_lanes() {
        let (options_json, use_fjall) =
            options_json_with_parallelism(r#"{"use_fjall":true}"#, 4).unwrap();
        let opts: serde_json::Value = serde_json::from_str(&options_json).unwrap();

        assert!(use_fjall);
        assert_eq!(opts["forks"], 4);
        assert_eq!(opts["inline_lookup"], false);
        assert!(opts.get("annotation_workers").is_none());
        assert!(opts.get("contig_parallelism").is_none());
        assert!(opts.get("chunked_buffer_lookup").is_none());
    }

    #[test]
    fn forks_zero_selects_inline_lookup() {
        let (options_json, use_fjall) =
            options_json_with_parallelism(r#"{"use_fjall":true}"#, 0).unwrap();
        let opts: serde_json::Value = serde_json::from_str(&options_json).unwrap();

        assert!(use_fjall);
        assert_eq!(opts["forks"], 0);
        assert_eq!(opts["inline_lookup"], true);
    }

    #[test]
    fn nonzero_forks_require_fjall() {
        let err = options_json_with_parallelism(r#"{}"#, 1).unwrap_err();
        assert!(err.to_string().contains("forks > 0 requires use_fjall"));
    }

    #[test]
    fn runtime_budget_tracks_forks() {
        assert_eq!(effective_runtime_threads(true, 6), 6);
    }

    #[test]
    fn strict_path_uses_one_runtime_worker() {
        assert_eq!(effective_runtime_threads(true, 0), 1);
    }
}
