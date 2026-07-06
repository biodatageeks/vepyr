from collections.abc import Callable, Iterator

import pyarrow as pa

def build_cache(
    cache_root: str,
    output_dir: str,
    partitions: int = 8,
    cache_format: str = "parquet",
    on_progress: Callable[[str, str, int, int, int], None] | None = None,
    cache_source_type: str = "ensembl",
    overwrite: bool = False,
) -> list[tuple[str, list[tuple[str, int]], tuple[int, int, int, float] | None]]:
    """Build all cache entities from an Ensembl VEP cache."""
    ...

def build_plugin_cache(
    manifest_path: str,
    source_path: str,
    variation_cache_dir: str,
    plugin_cache_root: str,
    chroms: list[str] | None = None,
    overwrite: bool = False,
) -> list[tuple[str, int, int, int]]:
    """Build a plugin cache from a source manifest. Returns (chrom, rows, warm, cold)."""
    ...

def annotate_vcf(
    vcf_path: str,
    cache_dir: str,
    output_path: str,
    options_json: str,
    show_progress: bool = True,
    compression: str = "",
    on_batch_written: Callable[[int, int, int], None] | None = None,
) -> int:
    """Annotate a VCF and write results directly to a VCF file.

    Returns the number of rows written.
    """
    ...

def create_annotator(
    vcf_path: str,
    cache_dir: str,
    options_json: str,
    skip_csq: bool = True,
    limit: int | None = None,
) -> StreamingAnnotator:
    """Create a streaming VEP annotator that yields PyArrow RecordBatches."""
    ...

class StreamingAnnotator:
    """A streaming annotator that yields PyArrow RecordBatches."""

    @property
    def schema(self) -> pa.Schema: ...
    def __iter__(self) -> Iterator[pa.RecordBatch]: ...
    def __next__(self) -> pa.RecordBatch: ...
