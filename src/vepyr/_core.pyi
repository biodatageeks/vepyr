def convert_entity(
    cache_root: str,
    output_dir: str,
    entity: str,
    partitions: int = 8,
    memory_limit_gb: int = 32,
) -> list[tuple[str, int]] | None:
    """Convert a single entity type from an Ensembl VEP cache to Parquet."""
    ...

def _register_vep(ctx_ptr: int) -> None:
    """Register VEP functions into a SessionContext via raw pointer.

    Called internally by register_vep(). Do not call directly.
    """
    ...
