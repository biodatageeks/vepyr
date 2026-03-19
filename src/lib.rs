use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use pyo3::prelude::*;

mod convert;

/// Thread-safe row counter shared between Rust and Python.
/// Rust increments it during conversion; Python polls it for progress.
#[pyclass]
struct ProgressCounter {
    inner: Arc<AtomicUsize>,
}

#[pymethods]
impl ProgressCounter {
    #[new]
    fn new() -> Self {
        Self {
            inner: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Read and reset the counter, returning rows written since last poll.
    fn poll(&self) -> usize {
        self.inner.swap(0, Ordering::Relaxed)
    }
}

/// Convert a single entity type from an Ensembl VEP cache to Parquet.
///
/// Releases the GIL during conversion. Use ProgressCounter.poll() from
/// a Python thread to track progress without GIL contention.
#[pyfunction]
#[pyo3(signature = (cache_root, output_dir, entity, partitions, counter))]
fn convert_entity(
    py: Python<'_>,
    cache_root: &str,
    output_dir: &str,
    entity: &str,
    partitions: usize,
    counter: &ProgressCounter,
) -> PyResult<Option<Vec<(String, usize)>>> {
    let arc = counter.inner.clone();
    // Release the GIL so Python can poll the counter from another thread
    let result = py.allow_threads(|| {
        convert::convert_entity(cache_root, output_dir, entity, partitions, arc)
    });
    match result {
        Ok(results) => Ok(Some(results)),
        Err(e) if e == "skipped" => Ok(None),
        Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e)),
    }
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ProgressCounter>()?;
    m.add_function(wrap_pyfunction!(convert_entity, m)?)?;
    Ok(())
}
