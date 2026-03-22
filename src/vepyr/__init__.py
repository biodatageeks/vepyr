from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl

from vepyr._core import convert_entity as _convert_entity
from vepyr._core import create_annotator as _create_annotator

__all__ = ["build_cache", "annotate"]

log = logging.getLogger(__name__)

# Ensembl FTP URL templates for VEP cache tarballs.
# {method_infix} is "" for vep, "_merged" for merged, "_refseq" for refseq.
# Release >=115 uses indexed_vep_cache/, older releases use vep/.
_ENSEMBL_FTP_PATHS = [
    "https://ftp.ensembl.org/pub/release-{release}/variation/indexed_vep_cache/{species}{method_infix}_vep_{release}_{assembly}.tar.gz",
    "https://ftp.ensembl.org/pub/release-{release}/variation/vep/{species}{method_infix}_vep_{release}_{assembly}.tar.gz",
]


def _download_with_progress(url: str, dest: str) -> None:
    """Download a file with a tqdm progress bar."""
    import http.client
    import urllib.parse

    from tqdm import tqdm

    filename = dest.rsplit("/", 1)[-1]
    log.info("Downloading %s", url)

    parsed = urllib.parse.urlparse(url)
    conn = http.client.HTTPSConnection(parsed.hostname, timeout=60)
    conn.request("GET", parsed.path, headers={"Accept-Encoding": "identity"})
    resp = conn.getresponse()

    if resp.status in (301, 302, 303, 307, 308):
        location = resp.getheader("Location")
        conn.close()
        if location:
            return _download_with_progress(location, dest)

    if resp.status != 200:
        conn.close()
        import urllib.error

        raise urllib.error.HTTPError(
            url, resp.status, resp.reason, resp.headers, None
        )

    total = int(resp.getheader("Content-Length", 0)) or None
    with (
        tqdm(
            total=total,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc=f"Downloading {filename}",
            miniters=1,
        ) as pbar,
        open(dest, "wb") as f,
    ):
        while True:
            buf = resp.read(8 * 1024 * 1024)
            if not buf:
                break
            f.write(buf)
            pbar.update(len(buf))
    conn.close()


def _download_cache(
    release: int,
    species: str,
    assembly: str,
    method: str,
    dest: str,
) -> None:
    """Try FTP URL patterns and download the cache tarball."""
    import urllib.error

    method_infix = "" if method == "vep" else f"_{method}"

    for pattern in _ENSEMBL_FTP_PATHS:
        url = pattern.format(
            release=release,
            species=species,
            assembly=assembly,
            method_infix=method_infix,
        )
        try:
            _download_with_progress(url, dest)
            return
        except urllib.error.HTTPError as e:
            if e.code == 404:
                log.debug("Not found: %s", url)
                continue
            raise
    raise FileNotFoundError(
        f"VEP cache not found for {species} {method} release {release} "
        f"assembly {assembly}. Browse available caches at "
        f"https://ftp.ensembl.org/pub/release-{release}/variation/"
    )


def build_cache(
    release: int,
    cache_dir: str,
    *,
    species: str = "homo_sapiens",
    assembly: str = "GRCh38",
    method: str = "vep",
    partitions: int = 8,
    memory_limit_gb: int = 32,
    local_cache: str | None = None,
) -> list[tuple[str, int]]:
    """Download an Ensembl VEP cache and convert it to optimized Parquet files.

    Parameters
    ----------
    release : int
        Ensembl release number (e.g. 115).
    cache_dir : str
        Root directory for cache data and Parquet output.
    species : str
        Species name (default: ``"homo_sapiens"``).
    assembly : str
        Genome assembly (default: ``"GRCh38"``).
    method : str
        Cache type: ``"vep"`` (default), ``"merged"``, or ``"refseq"``.
    partitions : int
        Number of DataFusion partitions for parallelism (default: 8).
    memory_limit_gb : int
        Memory limit in GB for DataFusion (default: 32).
    local_cache : str or None
        Path to an already-unpacked Ensembl VEP cache directory (the one
        containing ``info.txt``). When provided, downloading and extraction
        are skipped entirely.

    Returns
    -------
    list[tuple[str, int]]
        List of ``(parquet_file_path, row_count)`` for each written file.
    """
    import os
    import tarfile

    if method not in ("vep", "merged", "refseq"):
        raise ValueError(
            f"Invalid method '{method}'. Must be 'vep', 'merged', or 'refseq'."
        )

    # Version directory name: e.g. "115_GRCh38_vep"
    version_dir = f"{release}_{assembly}_{method}"

    if local_cache is not None:
        cache_root = local_cache
        if not os.path.isdir(cache_root):
            raise FileNotFoundError(
                f"Local cache directory not found: {cache_root}"
            )
        log.info("Using local cache: %s", cache_root)
    else:
        method_infix = "" if method == "vep" else f"_{method}"
        tarball_name = (
            f"{species}{method_infix}_vep_{release}_{assembly}.tar.gz"
        )
        tarball_path = os.path.join(cache_dir, tarball_name)
        cache_root = os.path.join(cache_dir, species, f"{release}_{assembly}")

        os.makedirs(cache_dir, exist_ok=True)

        if not os.path.isdir(cache_root):
            if not os.path.isfile(tarball_path):
                _download_cache(release, species, assembly, method, tarball_path)

            tarball_size_mb = os.path.getsize(tarball_path) / (1024 * 1024)
            log.info(
                "Extracting %s (%.0f MB) ...", tarball_name, tarball_size_mb
            )
            with tarfile.open(tarball_path) as tar:
                tar.extractall(path=cache_dir)
            log.info("Extracted to %s", cache_root)

            os.remove(tarball_path)

        if not os.path.isdir(cache_root):
            raise FileNotFoundError(
                f"Cache directory not found after extraction: {cache_root}"
            )

    # Output: parquet/<version_dir>/<entity>/chr1.parquet
    output_dir = os.path.join(cache_dir, "parquet", version_dir)

    import sys
    import time

    entities = [
        "variation", "transcript", "exon",
        "translation", "regulatory", "motif",
    ]

    _in_notebook = False
    try:
        from IPython import get_ipython

        shell = get_ipython()
        if shell is not None and shell.__class__.__name__ == "ZMQInteractiveShell":
            from IPython.display import display, HTML

            _in_notebook = True
    except ImportError:
        pass

    def _show_status(msg: str) -> None:
        if _in_notebook:
            display(HTML(f"<pre>{msg}</pre>"))
        else:
            log.info(msg)

    all_results: list[tuple[str, int]] = []
    for i, entity in enumerate(entities):
        _show_status(f"[{i+1}/{len(entities)}] Converting {entity} ...")
        t0 = time.time()
        result = _convert_entity(
            cache_root, output_dir, entity, partitions, memory_limit_gb
        )
        elapsed = time.time() - t0

        if result is None:
            _show_status(
                f"[{i+1}/{len(entities)}] {entity}: skipped (no source files)"
            )
            continue

        for path, rows in result:
            rate = f"{rows / elapsed:,.0f}" if elapsed > 0 else "?"
            rel_path = os.path.relpath(path, output_dir)
            _show_status(
                f"[{i+1}/{len(entities)}] {entity}: {rows:,} rows "
                f"in {elapsed:.1f}s ({rate} rows/s) -> {rel_path}"
            )
        all_results.extend(result)

    log.info("Done. Wrote %d Parquet files to %s", len(all_results), output_dir)
    return all_results




def annotate(
    vcf: str,
    cache_dir: str,
    *,
    # Annotation feature flags
    everything: bool = False,
    hgvs: bool = False,
    hgvsc: bool = False,
    hgvsp: bool = False,
    shift_hgvs: bool | None = None,
    no_escape: bool = False,
    remove_hgvsp_version: bool = False,
    hgvsp_use_prediction: bool = False,
    reference_fasta: str | None = None,
    # Co-located variant flags
    check_existing: bool = False,
    af: bool = False,
    af_1kg: bool = False,
    af_gnomade: bool = False,
    af_gnomadg: bool = False,
    max_af: bool = False,
    pubmed: bool = False,
    # Lookup tuning
    use_fjall: bool = False,
    extended_probes: bool = True,
    distance: int | tuple[int, int] | None = None,
    merged: bool = False,
    failed: int = 0,
    # Engine tuning
    cache_size_mb: int = 1024,
) -> "pl.LazyFrame":
    """Annotate variants from a VCF file with VEP consequences.

    Reads the VCF, runs ``annotate_vep()`` against the partitioned parquet
    cache produced by :func:`build_cache`, and returns a polars ``LazyFrame``.

    The engine auto-discovers context tables (transcript, exon, translation,
    regulatory, motif) from ``cache_dir`` subdirectories.

    Parameters
    ----------
    vcf : str
        Path to the input VCF file.
    cache_dir : str
        Path to the parquet cache directory produced by :func:`build_cache`,
        e.g. ``"/data/vep/wgs/parquet/115_GRCh38_vep"``.
    everything : bool
        Enable all annotation features (80-field CSQ). Implies ``hgvs``,
        ``af``, ``check_existing``, ``pubmed``, etc. Requires
        ``reference_fasta``.
    hgvs : bool
        Add HGVS notation. Implies ``hgvsc``, ``hgvsp``, ``shift_hgvs``.
        Requires ``reference_fasta``.
    hgvsc : bool
        Enable HGVSc notation (implied by ``hgvs``/``everything``).
    hgvsp : bool
        Enable HGVSp notation (implied by ``hgvs``/``everything``).
    shift_hgvs : bool or None
        3' shift HGVS notation. ``None`` = auto (True when hgvs enabled).
    no_escape : bool
        Don't URI-escape HGVS strings.
    remove_hgvsp_version : bool
        Remove version from HGVSp transcript ID.
    hgvsp_use_prediction : bool
        Use predicted rather than observed protein sequence.
    reference_fasta : str or None
        Path to reference FASTA (required for HGVS/everything).
    check_existing : bool
        Check for co-located known variants (implied by AF flags).
    af : bool
        Include allele frequencies.
    af_1kg : bool
        Include 1000 Genomes allele frequencies.
    af_gnomade : bool
        Include gnomAD exome allele frequencies.
    af_gnomadg : bool
        Include gnomAD genome allele frequencies.
    max_af : bool
        Include maximum AF across populations.
    pubmed : bool
        Include PubMed IDs for co-located variants.
    extended_probes : bool
        Use interval-overlap fallback for shifted indels (default: True).
    distance : int or tuple[int, int] or None
        Upstream/downstream distance for transcript overlap. Single int =
        both directions; tuple = (upstream, downstream).
    merged : bool
        Use merged Ensembl+RefSeq cache.
    failed : int
        Maximum allowed ``failed`` flag value from cache (default: 0).
    cache_size_mb : int
        Annotation cache size in MB (default: 1024).

    Returns
    -------
    polars.LazyFrame
        Annotated variants with ``csq`` and ``most_severe_consequence``
        columns, plus original VCF fields.

    Examples
    --------
    >>> import vepyr
    >>> lf = vepyr.annotate("input.vcf", "/data/vep/parquet/115_GRCh38_vep")
    >>> lf.collect()

    >>> # Full annotation with all features
    >>> lf = vepyr.annotate(
    ...     "input.vcf",
    ...     "/data/vep/parquet/115_GRCh38_vep",
    ...     everything=True,
    ...     reference_fasta="/ref/GRCh38.fa",
    ... )

    >>> # Selective: HGVS + allele frequencies
    >>> lf = vepyr.annotate(
    ...     "input.vcf",
    ...     "/data/vep/parquet/115_GRCh38_vep",
    ...     hgvs=True,
    ...     af=True,
    ...     af_gnomadg=True,
    ...     reference_fasta="/ref/GRCh38.fa",
    ... )
    """
    import json

    # Validate reference_fasta requirement
    if (everything or hgvs) and not reference_fasta:
        raise ValueError(
            "reference_fasta is required when everything=True or hgvs=True"
        )

    # Build options JSON — all flags pass through to the engine.
    opts: dict = {
        "extended_probes": extended_probes,
    }

    if use_fjall:
        opts["use_fjall"] = True

    if everything:
        opts["everything"] = True
    if hgvs:
        opts["hgvs"] = True
    if hgvsc:
        opts["hgvsc"] = True
    if hgvsp:
        opts["hgvsp"] = True
    if shift_hgvs is not None:
        opts["shift_hgvs"] = shift_hgvs
    if no_escape:
        opts["no_escape"] = True
    if remove_hgvsp_version:
        opts["remove_hgvsp_version"] = True
    if hgvsp_use_prediction:
        opts["hgvsp_use_prediction"] = True
    if reference_fasta:
        opts["reference_fasta_path"] = reference_fasta
    if check_existing:
        opts["check_existing"] = True
    if af:
        opts["af"] = True
    if af_1kg:
        opts["af_1kg"] = True
    if af_gnomade:
        opts["af_gnomade"] = True
    if af_gnomadg:
        opts["af_gnomadg"] = True
    if max_af:
        opts["max_af"] = True
    if pubmed:
        opts["pubmed"] = True
    if merged:
        opts["merged"] = True
    if failed != 0:
        opts["failed"] = failed
    if distance is not None:
        if isinstance(distance, tuple):
            opts["distance"] = f"{distance[0]},{distance[1]}"
        else:
            opts["distance"] = distance
    if cache_size_mb != 1024:
        opts["cache_size_mb"] = cache_size_mb

    options_json = json.dumps(opts)

    log.info("Running annotation on %s with cache %s", vcf, cache_dir)

    # Create a streaming annotator (own DataFusion session).
    # StreamingAnnotator is thread-safe (Mutex-wrapped) so polars can
    # call __next__ from any thread via register_io_source.
    annotator = _create_annotator(vcf, cache_dir, options_json)

    import polars as pl
    import pyarrow as pa

    # Convert PyArrow schema to polars schema
    pa_schema = annotator.schema
    empty = pa.table(
        {field.name: pa.array([], type=field.type) for field in pa_schema}
    )
    polars_schema = dict(pl.from_arrow(empty).schema)

    # Stream batches from Rust → polars via IO source plugin
    def _batch_source(with_columns, predicate, n_rows, batch_size):
        for py_batch in annotator:
            batch_df = pl.from_arrow(py_batch)
            if with_columns is not None:
                batch_df = batch_df.select(with_columns)
            if predicate is not None:
                batch_df = batch_df.filter(predicate)
            yield batch_df

    from polars.io.plugins import register_io_source

    return register_io_source(
        io_source=_batch_source,
        schema=polars_schema,
    )


def _sql_escape(s: str) -> str:
    """Escape single quotes in SQL string literals."""
    return s.replace("'", "''")
