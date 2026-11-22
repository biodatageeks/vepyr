use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Array, Int8Array, RecordBatch, StringArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_bio_function_vep::parquet_cache::write::VariationParquetShardWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use relayout::{relayout, settings_differences, summarize, Layout};

/// Warm rows (tier 0) then cold rows (tier 1), each sorted by start, as the builder writes them.
fn shard(dir: &Path) -> PathBuf {
    let mut meta = HashMap::new();
    meta.insert("vepyr.test".to_string(), "kept".to_string());
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("tier", DataType::Int8, false),
            Field::new("start", DataType::UInt32, false),
            Field::new("allele_string", DataType::Utf8, true),
        ],
        meta,
    ));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int8Array::from(vec![0, 0, 0, 1, 1, 1, 1])),
            Arc::new(UInt32Array::from(vec![5, 9, 30, 1, 5, 7, 40])),
            Arc::new(StringArray::from(vec![
                "w5", "w9", "w30", "c1", "c5", "c7", "c40",
            ])),
        ],
    )
    .unwrap();
    let path = dir.join("in.parquet");
    let mut w = VariationParquetShardWriter::create(&path, schema).unwrap();
    w.write(&batch).unwrap();
    w.finish().unwrap();
    path
}

fn read(path: &Path) -> RecordBatch {
    let file = std::fs::File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
    arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap()
}

fn col_u32(b: &RecordBatch, name: &str) -> Vec<u32> {
    let a = b.column(b.schema().index_of(name).unwrap());
    a.as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap()
        .values()
        .to_vec()
}

fn col_i8(b: &RecordBatch, name: &str) -> Vec<i8> {
    let a = b.column(b.schema().index_of(name).unwrap());
    a.as_any()
        .downcast_ref::<Int8Array>()
        .unwrap()
        .values()
        .to_vec()
}

fn col_str(b: &RecordBatch, name: &str) -> Vec<String> {
    let a = b.column(b.schema().index_of(name).unwrap());
    let s = a.as_any().downcast_ref::<StringArray>().unwrap();
    (0..s.len()).map(|i| s.value(i).to_string()).collect()
}

fn rows_without_tier(b: &RecordBatch) -> Vec<(u32, String)> {
    let mut v: Vec<(u32, String)> = col_u32(b, "start")
        .into_iter()
        .zip(col_str(b, "allele_string"))
        .collect();
    v.sort();
    v
}

#[test]
fn tiered_keeps_row_order() {
    let dir = tempfile::tempdir().unwrap();
    let input = shard(dir.path());
    let out = dir.path().join("tiered.parquet");
    let s = relayout(&input, &out, Layout::Tiered, 3).unwrap();
    let (a, b) = (read(&input), read(&out));
    assert_eq!(col_u32(&a, "start"), col_u32(&b, "start"));
    assert_eq!(col_i8(&a, "tier"), col_i8(&b, "tier"));
    assert_eq!(col_str(&a, "allele_string"), col_str(&b, "allele_string"));
    assert_eq!(s.rows, 7);
    assert_eq!(s.runs, 2);
}

#[test]
fn sorted_is_monotonic_single_tier() {
    let dir = tempfile::tempdir().unwrap();
    let input = shard(dir.path());
    let out = dir.path().join("sorted.parquet");
    let s = relayout(&input, &out, Layout::Sorted, 3).unwrap();
    let b = read(&out);
    assert_eq!(col_u32(&b, "start"), vec![1, 5, 5, 7, 9, 30, 40]);
    assert!(col_i8(&b, "tier").iter().all(|&t| t == 1));
    // Stable: the warm row with start 5 stays before the cold one.
    assert_eq!(
        col_str(&b, "allele_string")[1..3],
        ["w5".to_string(), "c5".to_string()]
    );
    assert_eq!(s.runs, 1);
    assert_eq!(summarize(&out).unwrap(), s);
}

#[test]
fn same_rows_both_layouts() {
    let dir = tempfile::tempdir().unwrap();
    let input = shard(dir.path());
    let t = dir.path().join("t.parquet");
    let s = dir.path().join("s.parquet");
    relayout(&input, &t, Layout::Tiered, 4).unwrap();
    relayout(&input, &s, Layout::Sorted, 4).unwrap();
    let base = rows_without_tier(&read(&input));
    assert_eq!(rows_without_tier(&read(&t)), base);
    assert_eq!(rows_without_tier(&read(&s)), base);
}

#[test]
fn metadata_preserved() {
    let dir = tempfile::tempdir().unwrap();
    let input = shard(dir.path());
    let out = dir.path().join("sorted.parquet");
    relayout(&input, &out, Layout::Sorted, 3).unwrap();
    let (a, b) = (read(&input), read(&out));
    assert_eq!(a.schema(), b.schema());
    // The engine writer decides the file's key-value metadata; the rewrite must not change it.
    let kv = |p: &Path| {
        let r = SerializedFileReader::new(std::fs::File::open(p).unwrap()).unwrap();
        r.metadata().file_metadata().key_value_metadata().cloned()
    };
    assert_eq!(kv(&input), kv(&out));
    let r = SerializedFileReader::new(std::fs::File::open(&out).unwrap()).unwrap();
    let sorting = r.metadata().row_group(0).sorting_columns().unwrap().clone();
    assert_eq!(
        sorting.iter().map(|c| c.column_idx).collect::<Vec<_>>(),
        vec![0, 1]
    );
}

#[test]
fn writer_params_identical() {
    let dir = tempfile::tempdir().unwrap();
    let input = shard(dir.path());
    let t = dir.path().join("t.parquet");
    let s = dir.path().join("s.parquet");
    relayout(&input, &t, Layout::Tiered, 3).unwrap();
    relayout(&input, &s, Layout::Sorted, 3).unwrap();
    assert_eq!(settings_differences(&t, &s).unwrap(), Vec::<String>::new());
    assert_eq!(
        settings_differences(&input, &t).unwrap(),
        Vec::<String>::new()
    );
}

#[test]
fn settings_difference_is_reported() {
    // A file written with other settings (default ArrowWriter, dictionary on, no page stats)
    // must be reported as different.
    let dir = tempfile::tempdir().unwrap();
    let input = shard(dir.path());
    let other = dir.path().join("other.parquet");
    let batch = read(&input);
    let mut w = parquet::arrow::ArrowWriter::try_new(
        std::fs::File::create(&other).unwrap(),
        batch.schema(),
        None,
    )
    .unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let diffs = settings_differences(&input, &other).unwrap();
    assert!(!diffs.is_empty(), "expected differences, got none");
}
