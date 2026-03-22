use std::sync::Arc;

use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_bio_function_vep::register_vep_functions;

/// Run annotate_vep SQL on a standalone DataFusion session.
/// Returns Arrow RecordBatches as PyArrow.
pub fn run_annotate(
    vcf_path: &str,
    cache_dir: &str,
    options_json: &str,
) -> Result<Vec<datafusion::arrow::array::RecordBatch>, String> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| format!("Failed to create runtime: {e}"))?;

    rt.block_on(async {
        let config = SessionConfig::new().with_target_partitions(1);
        let ctx = SessionContext::new_with_config(config);
        register_vep_functions(&ctx);

        // Register VCF using datafusion-bio-format-vcf
        let vcf_provider = datafusion_bio_format_vcf::table_provider::VcfTableProvider::new(
            vcf_path.to_string(),
            Some(vec![]),
            Some(vec![]),
            None,
            false,
        )
        .map_err(|e| format!("Failed to open VCF: {e}"))?;
        ctx.register_table("vcf", Arc::new(vcf_provider))
            .map_err(|e| format!("Failed to register VCF: {e}"))?;

        let sql = format!(
            "SELECT * FROM annotate_vep('vcf', '{}', 'parquet', '{}')",
            cache_dir.replace('\'', "''"),
            options_json.replace('\'', "''"),
        );

        let df = ctx.sql(&sql).await.map_err(|e| format!("SQL failed: {e}"))?;
        let batches = df.collect().await.map_err(|e| format!("Collect failed: {e}"))?;
        Ok(batches)
    })
}
