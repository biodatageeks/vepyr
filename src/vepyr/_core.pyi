def cache_to_parquet(
    cache_root: str,
    output_dir: str,
    partitions: int = 8,
) -> list[tuple[str, int]]:
    """Convert an Ensembl VEP cache directory to Parquet files.

    Args:
        cache_root: Path to unpacked Ensembl VEP cache (contains info.txt).
        output_dir: Output directory for Parquet files.
        partitions: DataFusion parallelism level.

    Returns:
        List of (file_path, row_count) for written files.
    """
    ...
