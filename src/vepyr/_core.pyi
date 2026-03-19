class ProgressCounter:
    """Thread-safe row counter shared between Rust and Python."""

    def __init__(self) -> None: ...
    def poll(self) -> int:
        """Read and reset the counter. Returns rows written since last poll."""
        ...

def convert_entity(
    cache_root: str,
    output_dir: str,
    entity: str,
    partitions: int,
    counter: ProgressCounter,
) -> list[tuple[str, int]] | None:
    """Convert a single entity type from an Ensembl VEP cache to Parquet.

    Releases the GIL during conversion. Poll counter from a Python thread.

    Args:
        cache_root: Path to unpacked Ensembl VEP cache (contains info.txt).
        output_dir: Output directory for Parquet files.
        entity: One of: variation, transcript, exon, translation, regulatory, motif.
        partitions: DataFusion parallelism level.
        counter: Shared atomic counter for progress tracking.

    Returns:
        List of (file_path, row_count) for written files, or None if skipped.
    """
    ...
