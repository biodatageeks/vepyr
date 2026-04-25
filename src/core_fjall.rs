//! Thin wrapper around the shared core cache builder in
//! `datafusion-bio-function-vep` for rebuilding Fjall stores from parquet.

use datafusion_bio_function_vep::cache_builder::CacheBuilder;

pub fn build_entity_fjall(
    cache_root: &str,
    output_dir: &str,
    entity: &str,
    partitions: usize,
    chromosomes: Option<Vec<String>>,
) -> Result<Vec<(String, usize)>, String> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| format!("Failed to create runtime for core fjall build: {e}"))?;

    rt.block_on(async move {
        let mut builder = CacheBuilder::new(cache_root, output_dir)
            .with_partitions(partitions)
            .with_build_fjall(true);

        if let Some(selected) = chromosomes {
            builder = builder.with_chromosomes(selected);
        }

        let stats = builder
            .build_entity(entity)
            .await
            .map_err(|e| format!("Failed to build core fjall for {entity}: {e}"))?;

        let mut outputs = Vec::new();
        for stat in stats {
            if let Some(fjall) = stat.fjall_stats {
                let path = match stat.entity.as_str() {
                    "variation" => format!("{output_dir}/variation.fjall"),
                    "translation_sift" => format!("{output_dir}/translation_sift.fjall"),
                    other => format!("{output_dir}/{other}.fjall"),
                };
                outputs.push((path, fjall.total_positions as usize));
            }
        }
        Ok(outputs)
    })
}
