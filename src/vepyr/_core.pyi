from collections.abc import Callable, Iterator

import pyarrow as pa

def supported_vep_targets_json() -> str:
    """Return the compiled Ensembl VEP/cache support matrix as JSON."""
    ...

def cache_contig_identity_json(
    cache_dir: str,
    chrom: str,
    expected_cache_version: str | None = None,
) -> str:
    """Validate one contig's Parquet cache identity and return it as JSON."""
    ...

def build_cache(
    cache_root: str,
    output_dir: str,
    partitions: int = 8,
    cache_format: str = "parquet",
    on_progress: Callable[[str, str, int, int, int], None] | None = None,
    cache_source_type: str = "ensembl",
    overwrite: bool = False,
    expected_cache_version: str | None = None,
) -> list[tuple[str, list[tuple[str, int]], tuple[int, int, int, float] | None]]:
    """Build all cache entities from an Ensembl VEP cache."""
    ...

def build_cache_entity(
    cache_root: str,
    output_dir: str,
    entity: str,
    partitions: int = 8,
    cache_source_type: str = "ensembl",
    overwrite: bool = True,
    expected_cache_version: str | None = None,
) -> list[tuple[str, list[tuple[str, int]], tuple[int, int, int, float] | None]]:
    """Build one raw cache entity from an Ensembl VEP cache."""
    ...

def build_plugin_cache(
    manifest_path: str,
    source_path: str | dict[str, str],
    variation_cache_dir: str,
    plugin_cache_root: str,
    chroms: list[str] | None = None,
    overwrite: bool = False,
    verify_source: str = "strict",
) -> tuple[list[tuple[str, int, int, int]], str]:
    """Build a plugin cache from a source manifest.

    Returns the per-chromosome ``(chrom, rows, warm, cold)`` tuples and the
    ``sources`` provenance this build recorded, as a JSON string.

    ``source_path`` is a path for a single-source manifest, or ``{part: path}``
    for a manifest declaring several ``[[source]]`` entries.

    ``verify_source`` is ``"strict"`` (hash each source and fail on a mismatch
    with the manifest's ``md5``), ``"warn"`` (hash, log, continue) or
    ``"skip"`` (never hash).
    """
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
