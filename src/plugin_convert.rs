//! Thin wrapper around the shared plugin cache builder in
//! `datafusion-bio-function-vep`.

use datafusion::common::DataFusionError;
use datafusion_bio_function_vep::plugin::PluginKind;
use datafusion_bio_function_vep::plugin_cache_builder::{
    convert_cadd_sources_to_parquet, convert_plugin_to_parquet,
};

pub fn convert_plugin(
    plugin_name: &str,
    source_path: &str,
    output_dir: &str,
    partitions: usize,
    memory_limit_gb: usize,
    chromosomes: Option<Vec<String>>,
    assume_sorted_input: bool,
    preview_rows: Option<usize>,
) -> Result<Vec<(String, usize)>, DataFusionError> {
    match plugin_name {
        "clinvar" => convert_plugin_to_parquet(
            "clinvar",
            source_path,
            output_dir,
            partitions,
            memory_limit_gb,
            chromosomes,
            assume_sorted_input,
            preview_rows,
        ),
        "cadd" => datafusion_bio_function_vep::plugin_cache_builder::convert_plugin(
            source_path,
            output_dir,
            PluginKind::Cadd,
            partitions,
            memory_limit_gb,
            assume_sorted_input,
            preview_rows,
        ),
        "spliceai" => convert_plugin_to_parquet(
            "spliceai",
            source_path,
            output_dir,
            partitions,
            memory_limit_gb,
            chromosomes,
            assume_sorted_input,
            preview_rows,
        ),
        "alphamissense" => convert_plugin_to_parquet(
            "alphamissense",
            source_path,
            output_dir,
            partitions,
            memory_limit_gb,
            chromosomes,
            assume_sorted_input,
            preview_rows,
        ),
        "dbnsfp" => convert_plugin_to_parquet(
            "dbnsfp",
            source_path,
            output_dir,
            partitions,
            memory_limit_gb,
            chromosomes,
            assume_sorted_input,
            preview_rows,
        ),
        other => Err(DataFusionError::Execution(format!(
            "Failed to convert plugin {other}: unknown plugin"
        ))),
    }
}

pub fn convert_cadd_plugin(
    snv_source_path: &str,
    indel_source_path: &str,
    output_dir: &str,
    partitions: usize,
    memory_limit_gb: usize,
    chromosomes: Option<Vec<String>>,
    assume_sorted_input: bool,
    preview_rows: Option<usize>,
) -> Result<Vec<(String, usize)>, DataFusionError> {
    convert_cadd_sources_to_parquet(
        snv_source_path,
        indel_source_path,
        output_dir,
        partitions,
        memory_limit_gb,
        chromosomes,
        assume_sorted_input,
        preview_rows,
    )
}
