from collections.abc import Callable, Iterator

import pyarrow as pa

def convert_entity(
    cache_root: str,
    output_dir: str,
    entity: str,
    partitions: int = 8,
    memory_limit_gb: int = 32,
    chromosomes: list[str] | None = None,
) -> list[tuple[str, int]] | None:
    """Convert a single entity type from an Ensembl VEP cache to Parquet."""
    ...

def convert_plugin(
    plugin_name: str,
    source_path: str,
    output_dir: str,
    partitions: int = 8,
    memory_limit_gb: int = 32,
    chromosomes: list[str] | None = None,
    assume_sorted_input: bool = False,
    preview_rows: int | None = None,
) -> list[tuple[str, int]]:
    """Convert a single plugin source file to Parquet."""
    ...

def convert_cadd_plugin(
    snv_source_path: str,
    indel_source_path: str,
    output_dir: str,
    partitions: int = 8,
    memory_limit_gb: int = 32,
    chromosomes: list[str] | None = None,
    assume_sorted_input: bool = False,
    preview_rows: int | None = None,
) -> list[tuple[str, int]]:
    """Convert CADD SNV and indel source files into one Parquet plugin cache."""
    ...

def build_entity_fjall(
    cache_root: str,
    output_dir: str,
    entity: str,
    partitions: int = 8,
    chromosomes: list[str] | None = None,
) -> list[tuple[str, int]]:
    """Build a core fjall cache from existing parquet files for one entity."""
    ...

def build_plugin_fjall(
    plugin_name: str,
    parquet_dir: str,
    output_path: str,
    partitions: int = 8,
    chromosomes: list[str] | None = None,
) -> tuple[str, int]:
    """Convert plugin Parquet files to a fjall point-lookup cache."""
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
