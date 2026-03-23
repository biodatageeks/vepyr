use std::collections::HashSet;
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

/// Main chromosomes that get their own parquet file.
const MAIN_CHROMS: &[&str] = &[
    "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17",
    "18", "19", "20", "21", "22", "X", "Y",
];

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

fn entity_subdir(kind: EnsemblEntityKind) -> &'static str {
    match kind {
        EnsemblEntityKind::Variation => "variation",
        EnsemblEntityKind::Transcript => "transcript",
        EnsemblEntityKind::Exon => "exon",
        EnsemblEntityKind::Translation => "translation",
        EnsemblEntityKind::RegulatoryFeature => "regulatory",
        EnsemblEntityKind::MotifFeature => "motif",
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

fn build_query(kind: EnsemblEntityKind, table_name: &str, chrom_filter: Option<&str>) -> String {
    let where_clause = chrom_filter
        .map(|c| format!(" WHERE chrom = '{c}'"))
        .unwrap_or_default();

    match kind {
        EnsemblEntityKind::Transcript => {
            format!(
                "SELECT * FROM (\
                    SELECT *, ROW_NUMBER() OVER (\
                        PARTITION BY stable_id \
                        ORDER BY cds_start NULLS LAST\
                    ) AS _rn \
                    FROM {table_name}{where_clause}\
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
                    FROM {table_name}{where_clause}\
                ) WHERE _rn = 1 \
                ORDER BY transcript_id, start"
            )
        }
        _ => {
            format!("SELECT * FROM {table_name}{where_clause} ORDER BY chrom, start")
        }
    }
}

/// Build a WHERE clause matching multiple chromosomes.
fn build_multi_chrom_filter(chroms: &[&str]) -> String {
    let list = chroms
        .iter()
        .map(|c| format!("'{c}'"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(" WHERE chrom IN ({list})")
}

fn build_query_multi_chrom(kind: EnsemblEntityKind, table_name: &str, chroms: &[&str]) -> String {
    let where_clause = build_multi_chrom_filter(chroms);

    match kind {
        EnsemblEntityKind::Transcript => {
            format!(
                "SELECT * FROM (\
                    SELECT *, ROW_NUMBER() OVER (\
                        PARTITION BY stable_id \
                        ORDER BY cds_start NULLS LAST\
                    ) AS _rn \
                    FROM {table_name}{where_clause}\
                ) WHERE _rn = 1 \
                ORDER BY chrom, start"
            )
        }
        EnsemblEntityKind::Translation => unreachable!(),
        EnsemblEntityKind::Exon => {
            format!(
                "SELECT * FROM (\
                    SELECT *, ROW_NUMBER() OVER (\
                        PARTITION BY transcript_id, exon_number \
                        ORDER BY stable_id NULLS LAST\
                    ) AS _rn \
                    FROM {table_name}{where_clause}\
                ) WHERE _rn = 1 \
                ORDER BY transcript_id, start"
            )
        }
        _ => {
            format!("SELECT * FROM {table_name}{where_clause} ORDER BY chrom, start")
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

/// Get chromosome names from schema metadata (set by upstream PR #134).
fn chroms_from_schema(schema: &SchemaRef) -> Option<Vec<String>> {
    schema
        .metadata()
        .get("bio.vep.chromosomes")
        .and_then(|json| serde_json::from_str(json).ok())
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
    eprintln!(
        "  {label}: {} rows [{:.1}s, {rate}]",
        format_rows(rows),
        elapsed
    );
}

/// Create a fresh session + provider for a given entity and cache root.
fn make_ctx_and_register(
    cache_root: &str,
    kind: EnsemblEntityKind,
    table_name: &str,
    partitions: usize,
) -> datafusion::common::Result<SessionContext> {
    let config = SessionConfig::new().with_target_partitions(partitions);
    let ctx = SessionContext::new_with_config(config);
    let mut options = EnsemblCacheOptions::new(cache_root);
    options.target_partitions = Some(partitions);
    options.max_storable_partitions = Some(2);
    let provider = EnsemblCacheTableProvider::for_entity(kind, options)?;
    ctx.register_table(table_name, provider)?;
    Ok(ctx)
}

/// Stream a query to a parquet writer. Returns row count.
async fn stream_to_writer(
    ctx: &SessionContext,
    query: &str,
    writer: &mut ArrowWriter<File>,
    needs_rn_drop: bool,
) -> datafusion::common::Result<usize> {
    let df = ctx.sql(query).await?;
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
    let mut rows = 0usize;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        if batch.num_rows() == 0 {
            continue;
        }
        rows += batch.num_rows();
        writer.write(&batch)?;
    }
    Ok(rows)
}

/// Create a parquet writer for a given file path and entity kind.
fn create_writer(
    path: &str,
    schema: &SchemaRef,
    kind: EnsemblEntityKind,
    sort_cols: &[&str],
    rg_override: Option<usize>,
) -> datafusion::common::Result<ArrowWriter<File>> {
    let props = writer_properties(kind, schema, sort_cols, rg_override);
    let file = File::create(path)
        .map_err(|e| datafusion::error::DataFusionError::Execution(format!("{e}")))?;
    ArrowWriter::try_new(file, schema.clone(), Some(props))
        .map_err(|e| datafusion::error::DataFusionError::Execution(format!("{e}")))
}

async fn write_translation_split(
    cache_root: &str,
    output_dir: &str,
    partitions: usize,
    chroms: &Option<Vec<String>>,
) -> datafusion::common::Result<Vec<(String, usize)>> {
    let table_name = "tl";
    let main_set: HashSet<&str> = MAIN_CHROMS.iter().copied().collect();

    // Determine which chroms to process
    let (main_chroms, other_chroms): (Vec<&str>, Vec<&str>) = match chroms {
        Some(all) => {
            let main: Vec<&str> = all
                .iter()
                .filter(|c| main_set.contains(c.as_str()))
                .map(|s| s.as_str())
                .collect();
            let other: Vec<&str> = all
                .iter()
                .filter(|c| !main_set.contains(c.as_str()))
                .map(|s| s.as_str())
                .collect();
            (main, other)
        }
        None => (MAIN_CHROMS.to_vec(), vec![]),
    };

    let mut results = Vec::new();

    // Process each main chromosome
    for chrom in &main_chroms {
        let ctx = make_ctx_and_register(
            cache_root,
            EnsemblEntityKind::Translation,
            table_name,
            partitions,
        )?;

        let dedup_query = format!(
            "SELECT * FROM (\
                SELECT *, ROW_NUMBER() OVER (\
                    PARTITION BY transcript_id \
                    ORDER BY cdna_coding_start NULLS LAST\
                ) AS _rn \
                FROM {table_name} WHERE chrom = '{chrom}'\
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
            continue;
        }

        let mem_table =
            datafusion::datasource::MemTable::try_new(deduped[0].schema(), vec![deduped])?;
        let split_ctx = SessionContext::new();
        split_ctx.register_table("_tl_deduped", Arc::new(mem_table))?;

        // translation_core
        let core_schema = datafusion_bio_format_ensembl_cache::translation_core_schema(false);
        let core_select = core_schema
            .fields()
            .iter()
            .map(|f| format!("\"{}\"", f.name()))
            .collect::<Vec<_>>()
            .join(", ");
        let core_file = format!("{output_dir}/translation_core/chr{chrom}.parquet");
        let core_query = format!("SELECT {core_select} FROM _tl_deduped ORDER BY transcript_id");

        let mut w = create_writer(
            &core_file,
            &core_schema,
            EnsemblEntityKind::Translation,
            &["transcript_id"],
            None,
        )?;
        let core_df = split_ctx.sql(&core_query).await?;
        let mut stream = core_df.execute_stream().await?;
        let mut core_rows = 0usize;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            if batch.num_rows() == 0 {
                continue;
            }
            let batch = project_batch(&batch, &core_schema)?;
            core_rows += batch.num_rows();
            w.write(&batch)?;
        }
        w.close()?;
        results.push((core_file, core_rows));

        // translation_sift
        let sift_schema = datafusion_bio_format_ensembl_cache::translation_sift_schema(false);
        let sift_select = sift_schema
            .fields()
            .iter()
            .map(|f| format!("\"{}\"", f.name()))
            .collect::<Vec<_>>()
            .join(", ");
        let sift_file = format!("{output_dir}/translation_sift/chr{chrom}.parquet");
        let sift_query = format!("SELECT {sift_select} FROM _tl_deduped ORDER BY chrom, start");

        let mut w = create_writer(
            &sift_file,
            &sift_schema,
            EnsemblEntityKind::Translation,
            &["chrom", "start"],
            Some(256),
        )?;
        let sift_df = split_ctx.sql(&sift_query).await?;
        let mut stream = sift_df.execute_stream().await?;
        let mut sift_rows = 0usize;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            if batch.num_rows() == 0 {
                continue;
            }
            let batch = project_batch(&batch, &sift_schema)?;
            sift_rows += batch.num_rows();
            w.write(&batch)?;
        }
        w.close()?;
        results.push((sift_file, sift_rows));

        split_ctx.deregister_table("_tl_deduped")?;
        eprintln!(
            "  translation: chr{chrom} core={} sift={}",
            format_rows(core_rows),
            format_rows(sift_rows)
        );
    }

    // Process remaining contigs as "other"
    if !other_chroms.is_empty() {
        let ctx = make_ctx_and_register(
            cache_root,
            EnsemblEntityKind::Translation,
            table_name,
            partitions,
        )?;
        let in_list = other_chroms
            .iter()
            .map(|c| format!("'{c}'"))
            .collect::<Vec<_>>()
            .join(", ");
        let dedup_query = format!(
            "SELECT * FROM (\
                SELECT *, ROW_NUMBER() OVER (\
                    PARTITION BY transcript_id \
                    ORDER BY cdna_coding_start NULLS LAST\
                ) AS _rn \
                FROM {table_name} WHERE chrom IN ({in_list})\
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

        if !deduped.is_empty() && deduped.iter().any(|b| b.num_rows() > 0) {
            let mem_table =
                datafusion::datasource::MemTable::try_new(deduped[0].schema(), vec![deduped])?;
            let split_ctx = SessionContext::new();
            split_ctx.register_table("_tl_deduped", Arc::new(mem_table))?;

            let core_schema = datafusion_bio_format_ensembl_cache::translation_core_schema(false);
            let core_select = core_schema
                .fields()
                .iter()
                .map(|f| format!("\"{}\"", f.name()))
                .collect::<Vec<_>>()
                .join(", ");
            let core_file = format!("{output_dir}/translation_core/other.parquet");
            let mut w = create_writer(
                &core_file,
                &core_schema,
                EnsemblEntityKind::Translation,
                &["transcript_id"],
                None,
            )?;
            let core_rows = stream_to_writer(
                &split_ctx,
                &format!("SELECT {core_select} FROM _tl_deduped ORDER BY transcript_id"),
                &mut w,
                false,
            )
            .await?;
            w.close()?;
            results.push((core_file, core_rows));

            let sift_schema = datafusion_bio_format_ensembl_cache::translation_sift_schema(false);
            let sift_select = sift_schema
                .fields()
                .iter()
                .map(|f| format!("\"{}\"", f.name()))
                .collect::<Vec<_>>()
                .join(", ");
            let sift_file = format!("{output_dir}/translation_sift/other.parquet");
            let mut w = create_writer(
                &sift_file,
                &sift_schema,
                EnsemblEntityKind::Translation,
                &["chrom", "start"],
                Some(256),
            )?;
            let sift_rows = stream_to_writer(
                &split_ctx,
                &format!("SELECT {sift_select} FROM _tl_deduped ORDER BY chrom, start"),
                &mut w,
                false,
            )
            .await?;
            w.close()?;
            results.push((sift_file, sift_rows));

            eprintln!(
                "  translation: other ({} contigs) core={} sift={}",
                other_chroms.len(),
                format_rows(core_rows),
                format_rows(sift_rows)
            );
        }
    }

    Ok(results)
}

/// Convert a single entity from the Ensembl VEP cache to Parquet.
/// Output: <output_dir>/<entity>/<prefix>_chr<N>.parquet for main chroms,
///         <output_dir>/<entity>/<prefix>_other.parquet for remaining contigs.
pub fn convert_entity(
    cache_root: &str,
    output_dir: &str,
    entity: &str,
    partitions: usize,
    _memory_limit_gb: usize,
) -> Result<Vec<(String, usize)>, String> {
    let kind = parse_entity(entity).ok_or_else(|| format!("Unknown entity: {entity}"))?;

    // Create entity subdirectory
    let subdir = entity_subdir(kind);
    let entity_dir = format!("{output_dir}/{subdir}");
    std::fs::create_dir_all(&entity_dir)
        .map_err(|e| format!("Failed to create dir {entity_dir}: {e}"))?;

    // For translation, also create the split subdirs
    if kind == EnsemblEntityKind::Translation {
        std::fs::create_dir_all(format!("{output_dir}/translation_core"))
            .map_err(|e| format!("Failed to create dir: {e}"))?;
        std::fs::create_dir_all(format!("{output_dir}/translation_sift"))
            .map_err(|e| format!("Failed to create dir: {e}"))?;
    }

    let rt =
        tokio::runtime::Runtime::new().map_err(|e| format!("Failed to create runtime: {e}"))?;

    match rt.block_on(convert_entity_per_chrom(
        cache_root, output_dir, kind, partitions,
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

async fn convert_entity_per_chrom(
    cache_root: &str,
    output_dir: &str,
    kind: EnsemblEntityKind,
    partitions: usize,
) -> datafusion::common::Result<Vec<(String, usize)>> {
    let table_name = match kind {
        EnsemblEntityKind::Variation => "var",
        EnsemblEntityKind::Transcript => "tx",
        EnsemblEntityKind::Exon => "exon",
        EnsemblEntityKind::Translation => "tl",
        EnsemblEntityKind::RegulatoryFeature => "reg",
        EnsemblEntityKind::MotifFeature => "motif",
    };

    // Get chromosomes from schema metadata
    let init_ctx = make_ctx_and_register(cache_root, kind, table_name, partitions)?;
    let provider_schema = {
        let table = init_ctx.table(table_name).await?;
        table.schema().inner().clone()
    };
    let chroms = chroms_from_schema(&provider_schema);
    drop(init_ctx);

    // Translation has special split handling
    if kind == EnsemblEntityKind::Translation {
        return write_translation_split(cache_root, output_dir, partitions, &chroms).await;
    }

    let subdir = entity_subdir(kind);
    let needs_rn_drop = matches!(
        kind,
        EnsemblEntityKind::Transcript | EnsemblEntityKind::Exon
    );
    let main_set: HashSet<&str> = MAIN_CHROMS.iter().copied().collect();

    let (main_chroms, other_chroms): (Vec<String>, Vec<String>) = match &chroms {
        Some(all) => {
            let main: Vec<String> = all
                .iter()
                .filter(|c| main_set.contains(c.as_str()))
                .cloned()
                .collect();
            let other: Vec<String> = all
                .iter()
                .filter(|c| !main_set.contains(c.as_str()))
                .cloned()
                .collect();
            (main, other)
        }
        None => (MAIN_CHROMS.iter().map(|s| s.to_string()).collect(), vec![]),
    };

    eprintln!(
        "  {subdir}: {} main chroms, {} other contigs",
        main_chroms.len(),
        other_chroms.len()
    );

    let mut all_results = Vec::new();
    let global_start = Instant::now();
    let mut total_rows: usize = 0;

    // Process each main chromosome as a separate parquet file
    for chrom in &main_chroms {
        let ctx = make_ctx_and_register(cache_root, kind, table_name, partitions)?;
        let query = build_query(kind, table_name, Some(chrom));
        let output_file = format!("{output_dir}/{subdir}/chr{chrom}.parquet");

        // Get schema from first batch
        let df = ctx.sql(&query).await?;
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
        let mut writer = create_writer(&output_file, &schema, kind, sk, None)?;

        let mut chrom_rows = 0usize;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            if batch.num_rows() == 0 {
                continue;
            }
            chrom_rows += batch.num_rows();
            writer.write(&batch)?;
        }
        writer.close()?;
        total_rows += chrom_rows;

        if chrom_rows > 0 {
            eprintln!(
                "  {subdir}: chr{chrom} {} rows (total: {})",
                format_rows(chrom_rows),
                format_rows(total_rows)
            );
        }
        all_results.push((output_file, chrom_rows));
    }

    // Process remaining contigs as "other.parquet"
    if !other_chroms.is_empty() {
        let ctx = make_ctx_and_register(cache_root, kind, table_name, partitions)?;
        let other_refs: Vec<&str> = other_chroms.iter().map(|s| s.as_str()).collect();
        let query = build_query_multi_chrom(kind, table_name, &other_refs);
        let output_file = format!("{output_dir}/{subdir}/other.parquet");

        let df = ctx.sql(&query).await?;
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
        let mut writer = create_writer(&output_file, &schema, kind, sk, None)?;

        let mut other_rows = 0usize;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            if batch.num_rows() == 0 {
                continue;
            }
            other_rows += batch.num_rows();
            writer.write(&batch)?;
        }
        writer.close()?;
        total_rows += other_rows;

        eprintln!(
            "  {subdir}: other ({} contigs) {} rows",
            other_chroms.len(),
            format_rows(other_rows)
        );
        all_results.push((output_file, other_rows));
    }

    print_progress(subdir, total_rows, global_start.elapsed().as_secs_f64());
    Ok(all_results)
}
