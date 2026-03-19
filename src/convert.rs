use std::fs::File;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::parquet::format::SortingColumn;
use datafusion::parquet::schema::types::ColumnPath;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_bio_format_ensembl_cache::{
    EnsemblCacheOptions, EnsemblCacheTableProvider, EnsemblEntityKind,
};
use futures::StreamExt;

fn parse_entity(name: &str) -> Option<EnsemblEntityKind> {
    match name {
        "variation" => Some(EnsemblEntityKind::Variation),
        "transcript" => Some(EnsemblEntityKind::Transcript),
        "exon" => Some(EnsemblEntityKind::Exon),
        "translation" => Some(EnsemblEntityKind::Translation),
        "regulatory" => Some(EnsemblEntityKind::RegulatoryFeature),
        "motif" => Some(EnsemblEntityKind::MotifFeature),
        _ => None,
    }
}

fn row_group_size(kind: EnsemblEntityKind) -> usize {
    match kind {
        EnsemblEntityKind::Variation => 100_000,
        EnsemblEntityKind::Transcript => 8_000,
        EnsemblEntityKind::Exon => 45_000,
        EnsemblEntityKind::Translation => 6_000,
        EnsemblEntityKind::RegulatoryFeature => 9_000,
        EnsemblEntityKind::MotifFeature => 10_000,
    }
}

fn sorting_columns_for(schema: &SchemaRef, sort_columns: &[&str]) -> Option<Vec<SortingColumn>> {
    let cols: Vec<SortingColumn> = sort_columns
        .iter()
        .filter_map(|name| {
            schema
                .column_with_name(name)
                .map(|(idx, _)| SortingColumn::new(idx as i32, false, false))
        })
        .collect();
    if cols.len() == sort_columns.len() {
        Some(cols)
    } else {
        None
    }
}

fn sort_key(kind: EnsemblEntityKind) -> &'static [&'static str] {
    match kind {
        EnsemblEntityKind::Exon => &["transcript_id", "start"],
        _ => &["chrom", "start"],
    }
}

fn writer_properties(
    kind: EnsemblEntityKind,
    schema: &SchemaRef,
    sort_columns: &[&str],
    rg_size_override: Option<usize>,
) -> WriterProperties {
    let rg_size = rg_size_override.unwrap_or_else(|| row_group_size(kind));
    let sorting = sorting_columns_for(schema, sort_columns);

    let mut builder = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_max_row_group_size(rg_size)
        .set_sorting_columns(sorting);

    if matches!(
        kind,
        EnsemblEntityKind::Translation | EnsemblEntityKind::Exon
    ) {
        builder = builder.set_column_bloom_filter_enabled(ColumnPath::from("transcript_id"), true);
    }

    builder.build()
}

fn build_dedup_query(kind: EnsemblEntityKind, table_name: &str) -> String {
    match kind {
        EnsemblEntityKind::Transcript => {
            format!(
                "SELECT * FROM (\
                    SELECT *, ROW_NUMBER() OVER (\
                        PARTITION BY stable_id \
                        ORDER BY cds_start NULLS LAST\
                    ) AS _rn \
                    FROM {table_name}\
                ) WHERE _rn = 1 \
                ORDER BY chrom, start"
            )
        }
        EnsemblEntityKind::Translation => unreachable!("use write_translation_split() instead"),
        EnsemblEntityKind::Exon => {
            format!(
                "SELECT * FROM (\
                    SELECT *, ROW_NUMBER() OVER (\
                        PARTITION BY transcript_id, exon_number \
                        ORDER BY stable_id NULLS LAST\
                    ) AS _rn \
                    FROM {table_name}\
                ) WHERE _rn = 1 \
                ORDER BY transcript_id, start"
            )
        }
        _ => {
            format!("SELECT * FROM {table_name} ORDER BY chrom, start")
        }
    }
}

fn project_batch(
    batch: &datafusion::arrow::array::RecordBatch,
    target_schema: &SchemaRef,
) -> datafusion::common::Result<datafusion::arrow::array::RecordBatch> {
    let source_schema = batch.schema();
    let mut columns = Vec::with_capacity(target_schema.fields().len());
    for field in target_schema.fields() {
        let (idx, _) = source_schema
            .column_with_name(field.name())
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Column '{}' not found in source batch",
                    field.name()
                ))
            })?;
        columns.push(batch.column(idx).clone());
    }
    Ok(datafusion::arrow::array::RecordBatch::try_new(
        target_schema.clone(),
        columns,
    )?)
}

fn format_rows(n: usize) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}k", n as f64 / 1_000.0)
    } else {
        format!("{n}")
    }
}

fn print_progress(label: &str, rows: usize, elapsed: f64) {
    let rate = if elapsed > 0.0 {
        format!("{}/s", format_rows((rows as f64 / elapsed) as usize))
    } else {
        "? rows/s".to_string()
    };
    eprintln!("  {label}: {} rows [{:.1}s, {rate}]", format_rows(rows), elapsed);
}

async fn write_translation_split(
    ctx: &SessionContext,
    table_name: &str,
    output_dir: &str,
    prefix: &str,
) -> datafusion::common::Result<Vec<(String, usize)>> {
    let dedup_query = format!(
        "SELECT * FROM (\
            SELECT *, ROW_NUMBER() OVER (\
                PARTITION BY transcript_id \
                ORDER BY cdna_coding_start NULLS LAST\
            ) AS _rn \
            FROM {table_name}\
        ) WHERE _rn = 1"
    );

    let df = ctx.sql(&dedup_query).await?;
    let schema = df.schema().clone();
    let cols: Vec<_> = schema
        .columns()
        .into_iter()
        .filter(|c| c.name() != "_rn")
        .collect();
    let df = df.select_columns(&cols.iter().map(|c| c.name()).collect::<Vec<_>>())?;
    let deduped = df.collect().await?;

    if deduped.is_empty() || deduped.iter().all(|b| b.num_rows() == 0) {
        return Ok(vec![]);
    }

    let mem_table = datafusion::datasource::MemTable::try_new(deduped[0].schema(), vec![deduped])?;
    ctx.register_table("_tl_deduped", Arc::new(mem_table))?;

    let mut results = Vec::new();

    // translation_core: sorted by transcript_id
    {
        let core_schema = datafusion_bio_format_ensembl_cache::translation_core_schema(false);
        let core_select = core_schema
            .fields()
            .iter()
            .map(|f| format!("\"{}\"", f.name()))
            .collect::<Vec<_>>()
            .join(", ");
        let core_query = format!("SELECT {core_select} FROM _tl_deduped ORDER BY transcript_id");
        let core_file = format!("{output_dir}/{prefix}_translation_core.parquet");
        let core_props = writer_properties(
            EnsemblEntityKind::Translation,
            &core_schema,
            &["transcript_id"],
            None,
        );

        let core_df = ctx.sql(&core_query).await?;
        let mut core_stream = core_df.execute_stream().await?;
        let file = File::create(&core_file)
            .map_err(|e| datafusion::error::DataFusionError::Execution(format!("{e}")))?;
        let mut writer = ArrowWriter::try_new(file, core_schema.clone(), Some(core_props))?;
        let mut core_rows = 0usize;
        let start = Instant::now();
        let mut last_report = Instant::now();

        eprintln!("  translation_core: reading cache...");
        while let Some(batch_result) = core_stream.next().await {
            let batch = batch_result?;
            if batch.num_rows() == 0 {
                continue;
            }
            let batch = project_batch(&batch, &core_schema)?;
            core_rows += batch.num_rows();
            writer.write(&batch)?;
            if last_report.elapsed().as_secs() >= 5 {
                print_progress("translation_core", core_rows, start.elapsed().as_secs_f64());
                last_report = Instant::now();
            }
        }
        writer.close()?;
        print_progress("translation_core", core_rows, start.elapsed().as_secs_f64());
        results.push((core_file, core_rows));
    }

    // translation_sift: sorted by (chrom, start)
    {
        let sift_schema = datafusion_bio_format_ensembl_cache::translation_sift_schema(false);
        let sift_select = sift_schema
            .fields()
            .iter()
            .map(|f| format!("\"{}\"", f.name()))
            .collect::<Vec<_>>()
            .join(", ");
        let sift_query = format!("SELECT {sift_select} FROM _tl_deduped ORDER BY chrom, start");
        let sift_file = format!("{output_dir}/{prefix}_translation_sift.parquet");
        let sift_props = writer_properties(
            EnsemblEntityKind::Translation,
            &sift_schema,
            &["chrom", "start"],
            Some(256),
        );

        let sift_df = ctx.sql(&sift_query).await?;
        let mut sift_stream = sift_df.execute_stream().await?;
        let file = File::create(&sift_file)
            .map_err(|e| datafusion::error::DataFusionError::Execution(format!("{e}")))?;
        let mut writer = ArrowWriter::try_new(file, sift_schema.clone(), Some(sift_props))?;
        let mut sift_rows = 0usize;
        let start = Instant::now();
        let mut last_report = Instant::now();

        eprintln!("  translation_sift: reading cache...");
        while let Some(batch_result) = sift_stream.next().await {
            let batch = batch_result?;
            if batch.num_rows() == 0 {
                continue;
            }
            let batch = project_batch(&batch, &sift_schema)?;
            sift_rows += batch.num_rows();
            writer.write(&batch)?;
            if last_report.elapsed().as_secs() >= 5 {
                print_progress("translation_sift", sift_rows, start.elapsed().as_secs_f64());
                last_report = Instant::now();
            }
        }
        writer.close()?;
        print_progress("translation_sift", sift_rows, start.elapsed().as_secs_f64());
        results.push((sift_file, sift_rows));
    }

    ctx.deregister_table("_tl_deduped")?;
    Ok(results)
}

/// Convert a single entity from the Ensembl VEP cache to Parquet.
pub fn convert_entity(
    cache_root: &str,
    output_dir: &str,
    entity: &str,
    partitions: usize,
    memory_limit_gb: usize,
) -> Result<Vec<(String, usize)>, String> {
    let kind = parse_entity(entity).ok_or_else(|| format!("Unknown entity: {entity}"))?;

    let prefix = std::path::Path::new(cache_root)
        .file_name()
        .and_then(|f| f.to_str())
        .unwrap_or("unknown")
        .to_string();

    std::fs::create_dir_all(output_dir)
        .map_err(|e| format!("Failed to create output dir: {e}"))?;

    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| format!("Failed to create runtime: {e}"))?;

    match rt.block_on(convert_entity_async(
        cache_root, output_dir, &prefix, kind, partitions, memory_limit_gb,
    )) {
        Ok(results) => Ok(results),
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("No source files discovered") {
                Err("skipped".to_string())
            } else {
                Err(format!("Failed to convert {entity}: {e}"))
            }
        }
    }
}

async fn convert_entity_async(
    cache_root: &str,
    output_dir: &str,
    prefix: &str,
    kind: EnsemblEntityKind,
    partitions: usize,
    memory_limit_gb: usize,
) -> datafusion::common::Result<Vec<(String, usize)>> {
    let config = SessionConfig::new().with_target_partitions(partitions);

    let pool = Arc::new(datafusion::execution::memory_pool::FairSpillPool::new(
        memory_limit_gb * 1024 * 1024 * 1024,
    ));
    let rt_env = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .with_temp_file_path(output_dir)
        .with_max_temp_directory_size(500 * 1024 * 1024 * 1024) // 500 GB spill limit
        .build_arc()
        .map_err(|e| datafusion::error::DataFusionError::Execution(format!("{e}")))?;
    let state = datafusion::execution::SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(rt_env)
        .build();
    let ctx = SessionContext::new_with_state(state);

    let mut options = EnsemblCacheOptions::new(cache_root);
    options.target_partitions = Some(partitions);
    // Limit concurrent partition parsing to cap memory usage
    options.max_storable_partitions = Some(2);
    let provider = EnsemblCacheTableProvider::for_entity(kind, options)?;

    let table_name = match kind {
        EnsemblEntityKind::Variation => "var",
        EnsemblEntityKind::Transcript => "tx",
        EnsemblEntityKind::Exon => "exon",
        EnsemblEntityKind::Translation => "tl",
        EnsemblEntityKind::RegulatoryFeature => "reg",
        EnsemblEntityKind::MotifFeature => "motif",
    };
    ctx.register_table(table_name, provider)?;

    if kind == EnsemblEntityKind::Translation {
        return write_translation_split(&ctx, table_name, output_dir, prefix).await;
    }

    let entity_label = match kind {
        EnsemblEntityKind::Variation => "variation",
        EnsemblEntityKind::Transcript => "transcript",
        EnsemblEntityKind::Exon => "exon",
        EnsemblEntityKind::RegulatoryFeature => "regulatory",
        EnsemblEntityKind::MotifFeature => "motif",
        EnsemblEntityKind::Translation => unreachable!(),
    };

    let output_file = format!("{output_dir}/{prefix}_{entity_label}.parquet");

    let query = build_dedup_query(kind, table_name);
    let df = ctx.sql(&query).await?;

    let needs_rn_drop = matches!(
        kind,
        EnsemblEntityKind::Transcript | EnsemblEntityKind::Exon
    );
    let df = if needs_rn_drop {
        let schema = df.schema().clone();
        let cols: Vec<_> = schema
            .columns()
            .into_iter()
            .filter(|c| c.name() != "_rn")
            .collect();
        df.select_columns(&cols.iter().map(|c| c.name()).collect::<Vec<_>>())?
    } else {
        df
    };

    let mut stream = df.execute_stream().await?;
    let schema = stream.schema();
    let sk = sort_key(kind);
    let props = writer_properties(kind, &schema, sk, None);

    let file = File::create(&output_file)
        .map_err(|e| datafusion::error::DataFusionError::Execution(format!("{e}")))?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;
    let mut total_rows: usize = 0;
    let start = Instant::now();
    let mut last_report = Instant::now();

    eprintln!("  {entity_label}: reading cache...");
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        if batch.num_rows() == 0 {
            continue;
        }
        total_rows += batch.num_rows();
        writer.write(&batch)?;
        // Report progress every 5 seconds
        if last_report.elapsed().as_secs() >= 5 {
            print_progress(entity_label, total_rows, start.elapsed().as_secs_f64());
            last_report = Instant::now();
        }
    }
    writer.close()?;
    print_progress(entity_label, total_rows, start.elapsed().as_secs_f64());

    Ok(vec![(output_file, total_rows)])
}
