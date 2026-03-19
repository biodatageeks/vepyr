def convert_entity(
    cache_root: str,
    output_dir: str,
    entity: str,
    partitions: int = 8,
) -> list[tuple[str, int]] | None:
    """Convert a single entity type from an Ensembl VEP cache to Parquet.

    Args:
        cache_root: Path to unpacked Ensembl VEP cache (contains info.txt).
        output_dir: Output directory for Parquet files.
        entity: One of: variation, transcript, exon, translation, regulatory, motif.
        partitions: DataFusion parallelism level.

    Returns:
        List of (file_path, row_count) for written files, or None if skipped.
    """
    ...
