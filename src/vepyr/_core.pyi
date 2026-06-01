from collections.abc import Callable, Iterator

import pyarrow as pa

def build_cache(
    cache_root: str,
    output_dir: str,
    partitions: int = 8,
    cache_format: str = "indexed_parquet",
    zstd_level: int = 3,
    dict_size_kb: int = 112,
    on_progress: Callable[[str, str, int, int, int], None] | None = None,
    cache_source_type: str = "ensembl",
    overwrite: bool = False,
    variation_af_threshold: float = 0.01,
    variation_position_radius: int = 1,
    variation_cold_row_group_rows: int = 8_192,
    variation_cold_data_page_rows: int = 1_024,
) -> list[tuple[str, list[tuple[str, int]], tuple[int, int, int, float] | None]]:
    """Build all cache entities from an Ensembl VEP cache to Parquet."""
    ...

def annotate_vcf(
    vcf_path: str,
    cache_dir: str,
    output_path: str,
    options_json: str,
    show_progress: bool = True,
    compression: str = "",
    on_batch_written: Callable[[int, int, int], None] | None = None,
    forks: int = 0,
    workers: int = 1,
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
    forks: int = 0,
    workers: int = 1,
) -> StreamingAnnotator:
    """Create a streaming VEP annotator that yields PyArrow RecordBatches."""
    ...

class StreamingAnnotator:
    """A streaming annotator that yields PyArrow RecordBatches."""

    @property
    def schema(self) -> pa.Schema: ...
    def __iter__(self) -> Iterator[pa.RecordBatch]: ...
    def __next__(self) -> pa.RecordBatch: ...
