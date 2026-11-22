//! Rewrite a vepyr variation shard in the current warm/cold order (`Tiered`) or sorted by
//! `start` only (`Sorted`, every `tier` = 1), always with the engine's own
//! `VariationParquetShardWriter`, and compare the Parquet settings of two shards.

use std::collections::BTreeSet;
use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int8Array, RecordBatch, UInt32Array};
use arrow::compute::{concat_batches, take};
use datafusion_bio_function_vep::parquet_cache::write::VariationParquetShardWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
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

fn read_all(path: &Path) -> Res<RecordBatch> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let schema = builder.schema().clone();
    let batches = builder.build()?.collect::<Result<Vec<_>, _>>()?;
    Ok(concat_batches(&schema, &batches)?)
}

fn sorted_by_start(batch: &RecordBatch) -> Res<RecordBatch> {
    let schema = batch.schema();
    let start = batch
        .column(schema.index_of("start")?)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or("start must be UInt32")?;
    let mut idx: Vec<u32> = (0..batch.num_rows() as u32).collect();
    // Stable: rows with equal start keep their current (tier) order.
    idx.sort_by_key(|&i| start.value(i as usize));
    let indices = UInt32Array::from(idx);
    let tier_idx = schema.index_of("tier")?;
    let columns = batch
        .columns()
        .iter()
        .enumerate()
        .map(|(i, c)| -> Res<ArrayRef> {
            if i == tier_idx {
                Ok(Arc::new(Int8Array::from(vec![1i8; batch.num_rows()])))
            } else {
                Ok(take(c.as_ref(), &indices, None)?)
            }
        })
        .collect::<Res<Vec<_>>>()?;
    Ok(RecordBatch::try_new(schema, columns)?)
}

pub fn relayout(input: &Path, output: &Path, layout: Layout, batch_rows: usize) -> Res<Summary> {
    let batch = read_all(input)?;
    let batch = match layout {
        Layout::Tiered => batch,
        Layout::Sorted => sorted_by_start(&batch)?,
    };
    let mut writer = VariationParquetShardWriter::create(output, batch.schema())?;
    let mut offset = 0;
    while offset < batch.num_rows() {
        let len = batch_rows.min(batch.num_rows() - offset);
        writer.write(&batch.slice(offset, len))?;
        offset += len;
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
