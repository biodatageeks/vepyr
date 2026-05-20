//! Thin wrapper around the shared plugin cache builder in
//! `datafusion-bio-function-vep`.

pub fn build_plugin_fjall(
    plugin_name: &str,
    parquet_dir: &str,
    output_path: &str,
    partitions: usize,
    chromosomes: Option<Vec<String>>,
) -> Result<(String, usize), String> {
    datafusion_bio_function_vep::plugin_cache_builder::build_plugin_fjall_from_parquet(
        plugin_name,
        parquet_dir,
        output_path,
        partitions,
        chromosomes,
    )
    .map_err(|e| format!("Failed to build plugin fjall for {plugin_name}: {e}"))
}
