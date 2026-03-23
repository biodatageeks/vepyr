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
use tokio::runtime::Runtime;

/// A streaming annotator that yields PyArrow RecordBatches.
/// Thread-safe: wraps the stream in a Mutex so polars can call from any thread.
#[pyclass]
pub struct StreamingAnnotator {
    rt: std::sync::Arc<Runtime>,
    stream: std::sync::Mutex<Option<SendableRecordBatchStream>>,
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
    _py: Python<'_>,
    vcf_path: &str,
    cache_dir: &str,
    output_path: &str,
    options_json: &str,
    show_progress: bool,
    compression: &str,
    on_batch_written: Option<PyObject>,
) -> PyResult<usize> {
    let rt =
        Runtime::new().map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{e}")))?;

    let opts: Value = serde_json::from_str(options_json).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("Invalid options JSON: {e}"))
    })?;

    let backend = if opts
        .get("use_fjall")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        "fjall"
    } else {
        "parquet"
    };

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
        use_fjall: opts
            .get("use_fjall")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        hgvs: opts.get("hgvs").and_then(|v| v.as_bool()).unwrap_or(false),
        merged: opts
            .get("merged")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        failed: opts.get("failed").and_then(|v| v.as_i64()),
        distance: opts.get("distance").and_then(|v| {
            v.as_str()
                .map(String::from)
                .or_else(|| v.as_i64().map(|n| n.to_string()))
        }),
        compression: vcf_compression,
        show_progress,
        on_batch_written: callback,
    };

    rt.block_on(async {
        let rows = annotate_to_vcf(vcf_path, cache_dir, backend, output_path, &config)
            .await
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("VCF annotation failed: {e}"))
            })?;

        Ok(rows)
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
    let rt =
        Runtime::new().map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{e}")))?;

    let (stream, schema) = rt.block_on(async {
        let config = SessionConfig::new().with_target_partitions(1);
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
            "SELECT * EXCLUDE (csq)"
        } else {
            "SELECT *"
        };
        let limit_clause = limit.map(|n| format!(" LIMIT {n}")).unwrap_or_default();
        let sql = format!(
            "{select} FROM annotate_vep('vcf', '{}', 'parquet', '{}'){limit_clause}",
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
        rt: std::sync::Arc::new(rt),
        stream: std::sync::Mutex::new(Some(stream)),
        schema: py_schema,
    })
}
