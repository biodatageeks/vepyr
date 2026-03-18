use std::fs::File;
use std::sync::Arc;

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
use kdam::{Bar, BarExt};

fn entity_label(kind: EnsemblEntityKind) -> &'static str {
    match kind {
        EnsemblEntityKind::Variation => "variation",
        EnsemblEntityKind::Transcript => "transcript",
        EnsemblEntityKind::Exon => "exon",
        EnsemblEntityKind::Translation => "translation",
        EnsemblEntityKind::RegulatoryFeature => "regulatory",
        EnsemblEntityKind::MotifFeature => "motif",
    }
}

fn make_progress_bar(desc: &str) -> Bar {
    let mut pb = Bar::builder()
        .desc(desc)
        .unit(" rows")
        .unit_scale(true)
        .build()
        .unwrap();
    pb.mininterval = 0.3;
    pb
}

fn finish_progress_bar(pb: &mut Bar) {
    let _ = pb.refresh();
    eprintln!();
}

fn update_pb(pb: &mut Bar, n: usize) {
    let _ = pb.update(n);
}

/// Row group sizing per entity type.
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
        let mut pb = make_progress_bar("translation_core");

        while let Some(batch_result) = core_stream.next().await {
            let batch = batch_result?;
            if batch.num_rows() == 0 {
                continue;
            }
            let batch = project_batch(&batch, &core_schema)?;
            core_rows += batch.num_rows();
            update_pb(&mut pb, batch.num_rows());
            writer.write(&batch)?;
        }
        writer.close()?;
        finish_progress_bar(&mut pb);
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
        let mut pb = make_progress_bar("translation_sift");

        while let Some(batch_result) = sift_stream.next().await {
            let batch = batch_result?;
            if batch.num_rows() == 0 {
                continue;
            }
            let batch = project_batch(&batch, &sift_schema)?;
            sift_rows += batch.num_rows();
            update_pb(&mut pb, batch.num_rows());
            writer.write(&batch)?;
        }
        writer.close()?;
        finish_progress_bar(&mut pb);
        results.push((sift_file, sift_rows));
    }

    ctx.deregister_table("_tl_deduped")?;
    Ok(results)
}

/// Convert a single entity from the Ensembl VEP cache to Parquet.
async fn convert_entity(
    cache_root: &str,
    output_dir: &str,
    prefix: &str,
    kind: EnsemblEntityKind,
    partitions: usize,
) -> datafusion::common::Result<Vec<(String, usize)>> {
    let config = SessionConfig::new().with_target_partitions(partitions);
    let ctx = SessionContext::new_with_config(config);

    let mut options = EnsemblCacheOptions::new(cache_root);
    options.target_partitions = Some(partitions);
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

    let label = entity_label(kind);
    let output_file = format!("{output_dir}/{prefix}_{label}.parquet");

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
    let mut pb = make_progress_bar(label);

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        if batch.num_rows() == 0 {
            continue;
        }
        total_rows += batch.num_rows();
        update_pb(&mut pb, batch.num_rows());
        writer.write(&batch)?;
    }
    writer.close()?;
    finish_progress_bar(&mut pb);

    Ok(vec![(output_file, total_rows)])
}

/// Convert all entity types from the Ensembl VEP cache to optimized Parquet files.
///
/// Returns a list of (file_path, row_count) for all written files.
pub fn cache_to_parquet(
    cache_root: &str,
    output_dir: &str,
    partitions: usize,
) -> Result<Vec<(String, usize)>, String> {
    let prefix = std::path::Path::new(cache_root)
        .file_name()
        .and_then(|f| f.to_str())
        .unwrap_or("unknown")
        .to_string();

    std::fs::create_dir_all(output_dir).map_err(|e| format!("Failed to create output dir: {e}"))?;

    let rt = tokio::runtime::Runtime::new().map_err(|e| format!("Failed to create runtime: {e}"))?;

    let entities = [
        EnsemblEntityKind::Variation,
        EnsemblEntityKind::Transcript,
        EnsemblEntityKind::Exon,
        EnsemblEntityKind::Translation,
        EnsemblEntityKind::RegulatoryFeature,
        EnsemblEntityKind::MotifFeature,
    ];

    let mut all_results = Vec::new();
    for kind in entities {
        match rt.block_on(convert_entity(
            cache_root, output_dir, &prefix, kind, partitions,
        )) {
            Ok(results) => all_results.extend(results),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("No source files discovered") {
                    log::info!("Skipping {:?}: no source files found", entity_label(kind));
                    continue;
                }
                return Err(format!("Failed to convert {:?}: {e}", entity_label(kind)));
            }
        }
    }

    Ok(all_results)
}
