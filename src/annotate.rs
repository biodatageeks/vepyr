use std::sync::Arc;

use arrow::pyarrow::ToPyArrow;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_bio_format_vcf::VcfCompressionType;
use datafusion_bio_function_vep::register_vep_functions;
use datafusion_bio_function_vep::vcf_sink::{annotate_to_vcf, AnnotateVcfConfig, OnBatchWritten};
use futures::StreamExt;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use serde_json::Value;
use tokio::runtime::{Builder, Runtime};

fn worker_thread_count(workers: usize) -> usize {
    workers.max(1)
}

fn runtime_for_workers(workers: usize) -> PyResult<Arc<Runtime>> {
    // DataFusion uses `tokio::task::block_in_place()` while resolving table
    // metadata. That requires Tokio's multi-thread scheduler even for the
    // serial (`workers=1`) path.
    let runtime = Builder::new_multi_thread()
        .worker_threads(worker_thread_count(workers))
        .build()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{e}")))?;

    Ok(Arc::new(runtime))
}

fn normalize_options(options_json: &str) -> PyResult<(String, String)> {
    let mut opts: Value = serde_json::from_str(options_json).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("Invalid options JSON: {e}"))
    })?;
    let object = opts
        .as_object_mut()
        .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("options JSON must be an object"))?;
    let cache_format = object
        .get("cache_format")
        .and_then(|v| v.as_str())
        .unwrap_or("indexed_parquet")
        .to_string();
    if !matches!(
        cache_format.as_str(),
        "indexed_parquet" | "legacy_fjall" | "parquet"
    ) {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "cache_format must be 'indexed_parquet', 'legacy_fjall', or 'parquet'",
        ));
    }
    object.insert(
        "cache_format".to_string(),
        Value::from(cache_format.clone()),
    );
    let options_json = serde_json::to_string(&opts).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("Invalid options JSON: {e}"))
    })?;
    Ok((options_json, cache_format))
}

fn workers_from_options(opts: &Value) -> usize {
    opts.get("workers")
        .and_then(|v| v.as_u64())
        .and_then(|n| usize::try_from(n).ok())
        .filter(|n| *n > 0)
        .unwrap_or(1)
}

/// A streaming annotator that yields PyArrow RecordBatches.
/// Thread-safe: wraps the stream in a Mutex so polars can call from any thread.
#[pyclass]
pub struct StreamingAnnotator {
    rt: std::sync::Arc<Runtime>,
    stream: std::sync::Mutex<Option<SendableRecordBatchStream>>,
    #[pyo3(get)]
    schema: Py<PyAny>,
}

#[pymethods]
impl StreamingAnnotator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
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
                    return Ok(Some(py_batch.into()));
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
    on_batch_written: Option<Py<PyAny>>,
) -> PyResult<usize> {
    log::info!(
        "annotate_to_vcf_file start: input={}, output={}, show_progress={}, compression={}",
        vcf_path,
        output_path,
        show_progress,
        compression
    );

    let (options_json, cache_format) = normalize_options(options_json)?;
    let opts: Value = serde_json::from_str(&options_json).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("Invalid options JSON: {e}"))
    })?;
    let workers = workers_from_options(&opts);

    // The annotation store uses the engine's fixed backend token, which upstream
    // still names "lance" (a vestigial identifier — the actual storage is
    // Parquet). The variation cache is Parquet, carried via `cache_format` in
    // `options_json`. Keep the two decoupled.
    let _ = &cache_format;
    let backend = "lance";
    let rt = runtime_for_workers(workers)?;

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
                Python::attach(|py| {
                    if let Err(e) = cb.call1(py, (batch_rows, total_rows, total_input)) {
                        log::warn!("on_batch_written callback error: {e}");
                    }
                });
            },
        )
    });

    // `AnnotateVcfConfig` is `#[non_exhaustive]`, so it is built by assignment
    // rather than a struct literal: a field the engine adds then defaults here
    // instead of failing this crate's build.
    #[allow(clippy::field_reassign_with_default)]
    let mut config = AnnotateVcfConfig::default();
    config.everything = opts
        .get("everything")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    config.extended_probes = opts
        .get("extended_probes")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    config.expected_cache_version = opts
        .get("expected_cache_version")
        .and_then(|v| v.as_str())
        .map(String::from);
    config.reference_fasta_path = opts
        .get("reference_fasta_path")
        .and_then(|v| v.as_str())
        .map(String::from);
    config.hgvs = opts.get("hgvs").and_then(|v| v.as_bool()).unwrap_or(false);
    config.hgvsc = opts.get("hgvsc").and_then(|v| v.as_bool()).unwrap_or(false);
    config.hgvsp = opts.get("hgvsp").and_then(|v| v.as_bool()).unwrap_or(false);
    config.shift_hgvs = opts.get("shift_hgvs").and_then(|v| v.as_bool());
    config.no_escape = opts
        .get("no_escape")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    config.remove_hgvsp_version = opts
        .get("remove_hgvsp_version")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    config.hgvsp_use_prediction = opts
        .get("hgvsp_use_prediction")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    config.merged = false;
    config.refseq = false;
    config.gencode_basic = opts
        .get("gencode_basic")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    config.gencode_primary = opts
        .get("gencode_primary")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    config.all_refseq = opts
        .get("all_refseq")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    config.exclude_predicted = opts
        .get("exclude_predicted")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    config.pick = opts.get("pick").and_then(|v| v.as_bool()).unwrap_or(false);
    config.pick_allele = opts
        .get("pick_allele")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    config.per_gene = opts
        .get("per_gene")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    config.pick_allele_gene = opts
        .get("pick_allele_gene")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    config.flag_pick = opts
        .get("flag_pick")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    config.flag_pick_allele = opts
        .get("flag_pick_allele")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    config.flag_pick_allele_gene = opts
        .get("flag_pick_allele_gene")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    config.pick_order = opts
        .get("pick_order")
        .and_then(|v| v.as_str())
        .map(String::from);
    config.failed = opts.get("failed").and_then(|v| v.as_i64());
    config.distance = opts.get("distance").and_then(|v| {
        v.as_str()
            .map(String::from)
            .or_else(|| v.as_i64().map(|n| n.to_string()))
    });
    config.buffer_size = opts
        .get("buffer_size")
        .and_then(|v| v.as_u64())
        .and_then(|n| usize::try_from(n).ok())
        .filter(|n| *n > 0)
        .unwrap_or(datafusion_bio_function_vep::vcf_sink::VEP_DEFAULT_BUFFER_SIZE);
    // Single annotation-concurrency knob (vepyr `workers` -> engine `workers`).
    // The engine derives its lookup parallelism from `workers`; the sink's
    // DataFusion `target_partitions` stays 1 so the annotated VCF is written
    // as a single ordered output (the streaming path, which polars drains in
    // parallel, sets its own SessionConfig partitions from `workers`).
    config.workers = workers;
    config.target_partitions = 1;
    config.compression = vcf_compression;
    config.show_progress = show_progress;
    config.on_batch_written = callback;
    config.plugin_cache_root = opts
        .get("plugin_cache_root")
        .and_then(|v| v.as_str())
        .map(std::path::PathBuf::from);
    // Recorded as a `tool` attribute in the output header's provenance
    // lines. The header key itself is fixed by the engine.
    config.provenance_tool_name = Some(env!("CARGO_PKG_NAME").to_string());
    config.provenance_tool_version = Some(env!("CARGO_PKG_VERSION").to_string());
    // Reproduce each input record's INFO key order and FORMAT key list in the
    // output. On unless the caller opts out: byte agreement with Ensembl VEP is
    // the point of the VCF path, and neither survives the typed columns.
    config.preserve_record_layout = opts
        .get("preserve_record_layout")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);

    // Release the GIL so the Python background thread (in __init__.py) can
    // let Jupyter's main thread pump display updates for tqdm progress bars.
    // The on_batch_written callback re-acquires the GIL via Python::with_gil().
    py.detach(|| {
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
pub fn create_streaming_annotator(
    py: Python<'_>,
    vcf_path: &str,
    cache_dir: &str,
    options_json: &str,
    skip_csq: bool,
    limit: Option<usize>,
) -> PyResult<StreamingAnnotator> {
    let (options_json, _cache_format) = normalize_options(options_json)?;
    let opts: Value = serde_json::from_str(&options_json).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("Invalid options JSON: {e}"))
    })?;
    let workers = workers_from_options(&opts);
    let rt = runtime_for_workers(workers)?;

    let (stream, schema) = rt.block_on(async {
        // Annotation store uses the engine's fixed backend token (vestigially
        // named "lance" upstream; storage is Parquet). The variation cache is
        // Parquet, selected by `cache_format` in options_json.
        let backend = "lance";
        let session_partitions = worker_thread_count(workers);

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
        schema: py_schema.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::{normalize_options, worker_thread_count, workers_from_options};

    #[test]
    fn normalize_preserves_workers_and_cache_format() {
        let (json, fmt) = normalize_options(r#"{"cache_format":"parquet","workers":4}"#).unwrap();
        let opts: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(fmt, "parquet");
        assert_eq!(opts["workers"], 4);
        assert_eq!(workers_from_options(&opts), 4);
    }

    #[test]
    fn default_cache_format_and_workers_when_absent() {
        let (_json, fmt) = normalize_options(r#"{}"#).unwrap();
        assert_eq!(fmt, "indexed_parquet");
        let opts: serde_json::Value = serde_json::from_str(r#"{}"#).unwrap();
        assert_eq!(workers_from_options(&opts), 1);
    }

    #[test]
    fn invalid_cache_format_is_rejected() {
        pyo3::Python::initialize();
        let err = normalize_options(r#"{"cache_format":"fjall"}"#).unwrap_err();
        assert!(err.to_string().contains("cache_format"));
    }

    #[test]
    fn worker_thread_count_is_at_least_one() {
        assert_eq!(worker_thread_count(0), 1);
        assert_eq!(worker_thread_count(8), 8);
    }
}
