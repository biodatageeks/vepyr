use std::sync::Arc;

use arrow::pyarrow::ToPyArrow;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_bio_function_vep::register_vep_functions;
use futures::StreamExt;
use pyo3::prelude::*;
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
                        .map_err(|e| {
                            pyo3::exceptions::PyRuntimeError::new_err(format!("{e}"))
                        })?;
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

/// Create a streaming annotator that yields PyArrow RecordBatches.
pub fn create_streaming_annotator(
    py: Python<'_>,
    vcf_path: &str,
    cache_dir: &str,
    options_json: &str,
) -> PyResult<StreamingAnnotator> {
    let rt = Runtime::new()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{e}")))?;

    let (stream, schema) = rt
        .block_on(async {
            let config = SessionConfig::new().with_target_partitions(1);
            let ctx = SessionContext::new_with_config(config);
            register_vep_functions(&ctx);

            let vcf_provider =
                datafusion_bio_format_vcf::table_provider::VcfTableProvider::new(
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
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "Failed to register VCF: {e}"
                    ))
                })?;

            let sql = format!(
                "SELECT * FROM annotate_vep('vcf', '{}', 'parquet', '{}')",
                cache_dir.replace('\'', "''"),
                options_json.replace('\'', "''"),
            );

            let df = ctx
                .sql(&sql)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("SQL: {e}")))?;

            let schema = df.schema().inner().clone();
            let stream = df.execute_stream().await.map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("Stream: {e}"))
            })?;

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
