from __future__ import annotations

import logging

from vepyr._core import convert_entity as _convert_entity
from vepyr._core import _register_vep

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


def _ensure_vep_registered() -> None:
    """Auto-register VEP functions into polars-bio's session (once)."""
    global _vep_registered
    if _vep_registered:
        return
    try:
        import polars_bio

        ctx = polars_bio.ctx
        if hasattr(ctx, "register_extension"):
            ctx.register_extension(_register_vep)
            _vep_registered = True
            log.info("VEP annotation functions registered into polars-bio session")
    except ImportError:
        pass  # polars-bio not installed, will error in annotate()


_vep_registered = False


def annotate(
    vcf: str,
    cache_dir: str,
    *,
    chrom: str | None = None,
    backend: str = "parquet",
    extended_probes: bool = False,
    everything: bool = False,
    hgvs: bool = False,
    shift_hgvs: bool = False,
    reference_fasta: str | None = None,
    cache_size_mb: int = 1024,
) -> object:
    """Annotate variants from a VCF file with VEP consequences.

    Reads the VCF, runs ``annotate_vep()`` against the parquet cache
    produced by :func:`build_cache`, and returns a polars ``LazyFrame``.

    Parameters
    ----------
    vcf : str
        Path to the input VCF file.
    cache_dir : str
        Path to the parquet cache directory produced by :func:`build_cache`,
        e.g. ``"/data/vep/wgs/parquet/115_GRCh38_vep"``.
    chrom : str or None
        Chromosome to annotate (e.g. ``"22"``). If None, annotates all.
    backend : str
        Cache backend: ``"parquet"`` (default) or ``"fjall"``.
    extended_probes : bool
        Use interval-overlap fallback for shifted indels.
    everything : bool
        Enable all annotation features (80-field CSQ).
    hgvs : bool
        Add HGVS notation to output.
    shift_hgvs : bool
        3' shift HGVS notation.
    reference_fasta : str or None
        Path to reference FASTA (required for HGVS).
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
    """
    import json
    import os

    try:
        import polars_bio as pb
    except ImportError:
        raise ImportError(
            "polars-bio is required for annotation. "
            "Install with: pip install polars-bio"
        ) from None

    _ensure_vep_registered()

    if not _vep_registered:
        raise RuntimeError(
            "Failed to register VEP functions. "
            "Ensure polars-bio >= 0.27 with register_extension() support."
        )

    # Register VCF as a table
    vcf_table = pb.read_vcf(vcf)
    vcf_table_name = "_vepyr_vcf"
    pb.from_polars(vcf_table, vcf_table_name)

    # Build variation cache source path
    if chrom is not None:
        variation_source = os.path.join(cache_dir, "variation", f"chr{chrom}.parquet")
    else:
        variation_source = os.path.join(cache_dir, "variation")

    # Build options JSON
    opts: dict = {
        "extended_probes": extended_probes,
    }

    if everything:
        opts["everything"] = True
    if hgvs:
        opts["hgvs"] = True
    if shift_hgvs:
        opts["shift_hgvs"] = True
    if reference_fasta:
        opts["reference_fasta_path"] = reference_fasta

    # Discover and register context tables
    context_tables = {
        "transcripts_table": "transcript",
        "exons_table": "exon",
        "translations_table": "translation_core",
        "translations_sift_table": "translation_sift",
        "regulatory_table": "regulatory",
        "motif_table": "motif",
    }

    for json_key, entity_dir in context_tables.items():
        entity_path = os.path.join(cache_dir, entity_dir)
        if os.path.isdir(entity_path):
            # Register the directory as a parquet table
            table_name = f"_vepyr_{entity_dir.replace('/', '_')}"
            pb.register_view(
                f"CREATE VIEW {table_name} AS "
                f"SELECT * FROM read_parquet('{entity_path}/*.parquet')",
            )
            opts[json_key] = table_name

    options_json = json.dumps(opts)

    # Run annotation SQL
    sql = (
        f"SELECT * FROM annotate_vep("
        f"'{vcf_table_name}', "
        f"'{_sql_escape(variation_source)}', "
        f"'{_sql_escape(backend)}', "
        f"'{_sql_escape(options_json)}')"
    )

    log.info("Running annotation: %s", sql)
    return pb.sql(sql)


def _sql_escape(s: str) -> str:
    """Escape single quotes in SQL string literals."""
    return s.replace("'", "''")
