//! Rewrite a vepyr variation shard in the current warm/cold order (`Tiered`) or sorted by
//! `start` only (`Sorted`, every `tier` = 1), always with the engine's own
//! `VariationParquetShardWriter`, and compare the Parquet settings of two shards.

use std::collections::BTreeSet;
use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int8Array, RecordBatch, UInt32Array};
use arrow::compute::interleave_record_batch;
use datafusion_bio_function_vep::parquet_cache::write::VariationParquetShardWriter;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReaderBuilder, RowSelection, RowSelector};
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::serialized_reader::ReadOptionsBuilder;

pub type Res<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Layout {
    Tiered,
    Sorted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Summary {
    pub rows: usize,
    /// Maximal non-decreasing runs of `start`.
    pub runs: usize,
    /// Pages of the `start` column (from the offset index).
    pub start_pages: usize,
    pub bytes: u64,
}

/// Rows of the warm run: the leading rows with `tier` = 0. Errors if a warm row follows a cold one.
fn warm_rows(path: &Path) -> Res<usize> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let leaf = (0..builder.parquet_schema().num_columns())
        .find(|&i| builder.parquet_schema().column(i).path().string() == "tier")
        .ok_or("no tier column")?;
    let mask = ProjectionMask::leaves(builder.parquet_schema(), [leaf]);
    let (mut warm, mut seen_cold) = (0usize, false);
    for b in builder.with_projection(mask).build()? {
        let b = b?;
        let t = b
            .column(0)
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or("tier must be Int8")?;
        for v in t.values().iter() {
            if *v == 0 {
                if seen_cold {
                    return Err("a warm row follows a cold row: not two runs".into());
                }
                warm += 1;
            } else {
                seen_cold = true;
            }
        }
    }
    Ok(warm)
}

type BatchIter = Box<dyn Iterator<Item = Res<RecordBatch>>>;

/// Batches of rows [first, first + len) of the file.
fn run_reader(path: &Path, first: usize, len: usize, read_rows: usize) -> Res<BatchIter> {
    if len == 0 {
        return Ok(Box::new(std::iter::empty()));
    }
    let selection = RowSelection::from(vec![RowSelector::skip(first), RowSelector::select(len)]);
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?
        .with_batch_size(read_rows)
        .with_row_selection(selection)
        .build()?;
    Ok(Box::new(reader.map(|b| b.map_err(|e| e.into()))))
}

fn starts(batch: &RecordBatch) -> Res<&UInt32Array> {
    batch
        .column(batch.schema().index_of("start")?)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| "start must be UInt32".into())
}

/// One sorted run being consumed: the batch it reads from (index into the buffer list) and row.
struct Run {
    reader: BatchIter,
    current: Option<(usize, usize)>,
    last: Option<u32>,
}

impl Run {
    fn refill(&mut self, bufs: &mut Vec<RecordBatch>) -> Res<()> {
        loop {
            if let Some((b, r)) = self.current {
                if r < bufs[b].num_rows() {
                    return Ok(());
                }
            }
            match self.reader.next() {
                Some(batch) => {
                    bufs.push(batch?);
                    self.current = Some((bufs.len() - 1, 0));
                }
                None => {
                    self.current = None;
                    return Ok(());
                }
            }
        }
    }

    fn peek(&self, bufs: &[RecordBatch]) -> Res<Option<u32>> {
        match self.current {
            Some((b, r)) => Ok(Some(starts(&bufs[b])?.value(r))),
            None => Ok(None),
        }
    }

    fn take(&mut self, bufs: &[RecordBatch]) -> Res<(usize, usize)> {
        let (b, r) = self.current.ok_or("run exhausted")?;
        let v = starts(&bufs[b])?.value(r);
        if self.last.is_some_and(|p| v < p) {
            return Err("a run is not sorted by start".into());
        }
        self.last = Some(v);
        self.current = Some((b, r + 1));
        Ok((b, r))
    }
}

fn with_cold_tier(batch: RecordBatch) -> Res<RecordBatch> {
    let schema = batch.schema();
    let tier_idx = schema.index_of("tier")?;
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns[tier_idx] = Arc::new(Int8Array::from(vec![1i8; batch.num_rows()]));
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn write_sliced(
    writer: &mut VariationParquetShardWriter,
    batch: &RecordBatch,
    batch_rows: usize,
) -> Res<()> {
    let mut offset = 0;
    while offset < batch.num_rows() {
        let len = batch_rows.min(batch.num_rows() - offset);
        writer.write(&batch.slice(offset, len))?;
        offset += len;
    }
    Ok(())
}

/// Stable two-way merge of the warm and the cold run by `start` (warm first on ties), streamed:
/// only the batches the current output chunk refers to are held in memory.
fn write_sorted(
    input: &Path,
    writer: &mut VariationParquetShardWriter,
    batch_rows: usize,
    read_rows: usize,
) -> Res<()> {
    let total = ParquetRecordBatchReaderBuilder::try_new(File::open(input)?)?
        .metadata()
        .file_metadata()
        .num_rows() as usize;
    let warm = warm_rows(input)?;
    let mut bufs: Vec<RecordBatch> = Vec::new();
    let mut runs = [
        Run {
            reader: run_reader(input, 0, warm, read_rows)?,
            current: None,
            last: None,
        },
        Run {
            reader: run_reader(input, warm, total - warm, read_rows)?,
            current: None,
            last: None,
        },
    ];
    let mut indices: Vec<(usize, usize)> = Vec::with_capacity(batch_rows);
    loop {
        for run in runs.iter_mut() {
            run.refill(&mut bufs)?;
        }
        let pick = match (runs[0].peek(&bufs)?, runs[1].peek(&bufs)?) {
            (None, None) => None,
            (Some(_), None) => Some(0),
            (None, Some(_)) => Some(1),
            (Some(x), Some(y)) => Some(if x <= y { 0 } else { 1 }),
        };
        if let Some(i) = pick {
            indices.push(runs[i].take(&bufs)?);
        }
        if indices.len() == batch_rows || (pick.is_none() && !indices.is_empty()) {
            let refs: Vec<&RecordBatch> = bufs.iter().collect();
            let chunk = interleave_record_batch(&refs, &indices)?;
            writer.write(&with_cold_tier(chunk)?)?;
            indices.clear();
            // Keep only the batches the runs still read from, and renumber them.
            let mut kept: Vec<RecordBatch> = Vec::new();
            for run in runs.iter_mut() {
                if let Some((b, r)) = run.current {
                    kept.push(bufs[b].clone());
                    run.current = Some((kept.len() - 1, r));
                }
            }
            bufs = kept;
        }
        if pick.is_none() {
            return Ok(());
        }
    }
}

pub fn relayout(input: &Path, output: &Path, layout: Layout, batch_rows: usize) -> Res<Summary> {
    relayout_with(input, output, layout, batch_rows, 8192)
}

/// As `relayout`, reading the input in batches of `read_rows` rows (memory stays bounded).
pub fn relayout_with(
    input: &Path,
    output: &Path,
    layout: Layout,
    batch_rows: usize,
    read_rows: usize,
) -> Res<Summary> {
    if batch_rows == 0 || read_rows == 0 {
        return Err("batch sizes must be positive".into());
    }
    let schema = ParquetRecordBatchReaderBuilder::try_new(File::open(input)?)?
        .schema()
        .clone();
    let mut writer = VariationParquetShardWriter::create(output, schema)?;
    match layout {
        Layout::Tiered => {
            let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(input)?)?
                .with_batch_size(read_rows)
                .build()?;
            for b in reader {
                write_sliced(&mut writer, &b?, batch_rows)?;
            }
        }
        Layout::Sorted => write_sorted(input, &mut writer, batch_rows, read_rows)?,
    }
    writer.finish()?;
    summarize(output)
}

fn metadata_with_page_index(path: &Path) -> Res<Arc<ParquetMetaData>> {
    let options = ReadOptionsBuilder::new().with_page_index().build();
    let reader = SerializedFileReader::new_with_options(File::open(path)?, options)?;
    Ok(Arc::new(reader.metadata().clone()))
}

fn start_leaf(meta: &ParquetMetaData) -> Res<usize> {
    let schema = meta.file_metadata().schema_descr();
    (0..schema.num_columns())
        .find(|&i| schema.column(i).path().string() == "start")
        .ok_or_else(|| "no start column".into())
}

pub fn summarize(path: &Path) -> Res<Summary> {
    let meta = metadata_with_page_index(path)?;
    let leaf = start_leaf(&meta)?;
    let offsets = meta.offset_index().ok_or("no offset index")?;
    let start_pages = offsets
        .iter()
        .map(|rg| rg[leaf].page_locations().len())
        .sum();

    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let mask = ProjectionMask::leaves(builder.parquet_schema(), [leaf]);
    let mut runs = 0usize;
    let mut prev: Option<u32> = None;
    for b in builder.with_projection(mask).build()? {
        let b = b?;
        let s = b
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or("start must be UInt32")?;
        for v in s.values().iter().copied() {
            if prev.is_none_or(|p| v < p) {
                runs += 1;
            }
            prev = Some(v);
        }
    }
    Ok(Summary {
        rows: meta.file_metadata().num_rows() as usize,
        runs,
        start_pages,
        bytes: std::fs::metadata(path)?.len(),
    })
}

/// One line per setting, independent of where page boundaries fall.
fn settings(path: &Path) -> Res<BTreeSet<String>> {
    let meta = metadata_with_page_index(path)?;
    let fm = meta.file_metadata();
    let mut out = BTreeSet::new();
    out.insert(format!("created_by={:?}", fm.created_by()));
    let mut kv: Vec<String> = fm
        .key_value_metadata()
        .map(|v| {
            v.iter()
                .map(|k| format!("{}={:?}", k.key, k.value))
                .collect()
        })
        .unwrap_or_default();
    kv.sort();
    out.insert(format!("key_value_metadata={kv:?}"));
    out.insert(format!("row_groups={}", meta.num_row_groups()));
    for (r, rg) in meta.row_groups().iter().enumerate() {
        out.insert(format!("row_group[{r}].rows={}", rg.num_rows()));
        out.insert(format!(
            "row_group[{r}].sorting_columns={:?}",
            rg.sorting_columns()
        ));
        for c in rg.columns() {
            let name = c.column_path().string();
            out.insert(format!(
                "column[{name}] codec={:?} encodings={:?} dictionary_page={} statistics={} column_index={} offset_index={}",
                c.compression(),
                c.encodings().collect::<Vec<_>>(),
                c.dictionary_page_offset().is_some(),
                c.statistics().is_some(),
                c.column_index_offset().is_some(),
                c.offset_index_offset().is_some(),
            ));
        }
    }
    Ok(out)
}

/// Settings that must agree between two shards (everything except page boundaries).
/// Each entry names a setting found in only one of the two files.
pub fn settings_differences(a: &Path, b: &Path) -> Res<Vec<String>> {
    let (sa, sb) = (settings(a)?, settings(b)?);
    let mut diffs: Vec<String> = sa
        .difference(&sb)
        .map(|s| format!("only in A: {s}"))
        .collect();
    diffs.extend(sb.difference(&sa).map(|s| format!("only in B: {s}")));
    Ok(diffs)
}

/// Rows and compressed bytes per page of every column: (column, pages, min/median/max rows,
/// min/median/max compressed bytes).
pub fn page_stats(path: &Path) -> Res<Vec<serde_json::Value>> {
    let meta = metadata_with_page_index(path)?;
    let offsets = meta.offset_index().ok_or("no offset index")?;
    let schema = meta.file_metadata().schema_descr();
    let mut out = Vec::new();
    for leaf in 0..schema.num_columns() {
        let mut rows = Vec::new();
        let mut bytes = Vec::new();
        for (r, rg) in offsets.iter().enumerate() {
            let locs = rg[leaf].page_locations();
            let n_rows = meta.row_group(r).num_rows();
            for (i, p) in locs.iter().enumerate() {
                let end = locs.get(i + 1).map(|q| q.first_row_index).unwrap_or(n_rows);
                rows.push(end - p.first_row_index);
                bytes.push(p.compressed_page_size as i64);
            }
        }
        let stat = |v: &mut Vec<i64>| {
            v.sort_unstable();
            serde_json::json!([v.first(), v.get(v.len() / 2), v.last()])
        };
        out.push(serde_json::json!({
            "column": schema.column(leaf).path().string(),
            "pages": rows.len(),
            "rows_min_median_max": stat(&mut rows),
            "bytes_min_median_max": stat(&mut bytes),
        }));
    }
    Ok(out)
}
