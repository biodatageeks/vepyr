from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl

from vepyr._core import annotate_vcf as _annotate_vcf
from vepyr._core import build_cache as _build_cache
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


_MAX_REDIRECTS = 5


def _download_with_progress(url: str, dest: str, _redirects: int = 0) -> None:
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
            if _redirects >= _MAX_REDIRECTS:
                raise RuntimeError(
                    f"Too many redirects ({_MAX_REDIRECTS}) fetching {url}"
                )
            return _download_with_progress(location, dest, _redirects + 1)

    if resp.status != 200:
        conn.close()
        import urllib.error

        raise urllib.error.HTTPError(url, resp.status, resp.reason, resp.headers, None)

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
    cache_type: str,
    dest: str,
) -> None:
    """Try FTP URL patterns and download the cache tarball."""
    import urllib.error

    method_infix = "" if cache_type == "vep" else f"_{cache_type}"

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
        f"VEP cache not found for {species} {cache_type} release {release} "
        f"assembly {assembly}. Browse available caches at "
        f"https://ftp.ensembl.org/pub/release-{release}/variation/"
    )


def build_cache(
    release: int,
    cache_dir: str,
    *,
    species: str = "homo_sapiens",
    assembly: str = "GRCh38",
    cache_type: str = "vep",
    partitions: int = 1,
    build_fjall: bool = True,
    fjall_zstd_level: int = 3,
    fjall_dict_size_kb: int = 112,
    local_cache: str | None = None,
    show_progress: bool = True,
    on_progress: "Callable[[str, str, int, int, int], None] | None" = None,
) -> list[tuple[str, int]]:
    """Download an Ensembl VEP cache and convert it to optimized Parquet + fjall.

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
    cache_type : str
        Cache type: ``"vep"`` (default), ``"merged"``, or ``"refseq"``.
    partitions : int
        Number of DataFusion partitions for parallelism (default: 1).
    build_fjall : bool
        Build fjall KV stores for variation and sift lookups (default: True).
    fjall_zstd_level : int
        Zstd compression level for fjall stores (default: 3).
    fjall_dict_size_kb : int
        Zstd dictionary size in KB for fjall stores (default: 112).
    local_cache : str or None
        Path to an already-unpacked Ensembl VEP cache directory (the one
        containing ``info.txt``). When provided, downloading and extraction
        are skipped entirely.
    show_progress : bool
        Show tqdm progress bars during conversion (default: True).
    on_progress : callable or None
        Custom progress callback with signature
        ``(entity, format, batch_rows, total_rows, total_expected)``.
        Overrides the default tqdm bars when provided.

    Returns
    -------
    list[tuple[str, int]]
        List of ``(parquet_file_path, row_count)`` for each written file.
    """
    import os
    import tarfile

    if cache_type not in ("vep", "merged", "refseq"):
        raise ValueError(
            f"Invalid cache_type '{cache_type}'. Must be 'vep', 'merged', or 'refseq'."
        )

    # Version directory name: e.g. "115_GRCh38_vep"
    version_dir = f"{release}_{assembly}_{cache_type}"

    if local_cache is not None:
        cache_root = local_cache
        if not os.path.isdir(cache_root):
            raise FileNotFoundError(f"Local cache directory not found: {cache_root}")
        log.info("Using local cache: %s", cache_root)
    else:
        method_infix = "" if cache_type == "vep" else f"_{cache_type}"
        tarball_name = f"{species}{method_infix}_vep_{release}_{assembly}.tar.gz"
        tarball_path = os.path.join(cache_dir, tarball_name)
        method_suffix = "" if cache_type == "vep" else f"_{cache_type}"
        cache_root = os.path.join(
            cache_dir, species, f"{release}_{assembly}{method_suffix}"
        )

        os.makedirs(cache_dir, exist_ok=True)

        if not os.path.isdir(cache_root):
            if not os.path.isfile(tarball_path):
                _download_cache(release, species, assembly, cache_type, tarball_path)

            tarball_size_mb = os.path.getsize(tarball_path) / (1024 * 1024)
            log.info("Extracting %s (%.0f MB) ...", tarball_name, tarball_size_mb)
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

    # Build progress callback: explicit wins, then auto-tqdm, then None.
    progress_cb = on_progress
    _bars: dict[tuple[str, str], object] | None = None

    if progress_cb is None and show_progress:
        try:
            from tqdm.auto import tqdm

            _bars = {}

            def progress_cb(
                entity: str,
                fmt: str,
                batch_rows: int,
                total_rows: int,
                total_expected: int,
            ) -> None:
                key = (entity, fmt)
                if key not in _bars:
                    _bars[key] = tqdm(
                        total=total_expected or None,
                        unit=" rows",
                        desc=f"{entity} ({fmt})",
                    )
                bar = _bars[key]
                bar.update(batch_rows)
        except ImportError:
            pass

    try:
        entity_stats = _build_cache(
            cache_root,
            output_dir,
            partitions,
            build_fjall,
            fjall_zstd_level,
            fjall_dict_size_kb,
            progress_cb,
        )
    finally:
        if _bars is not None:
            for bar in _bars.values():
                bar.close()

    # Flatten entity stats into the simple (path, rows) list for backward compat
    all_results: list[tuple[str, int]] = []
    for entity_name, parquet_files, fjall_stats in entity_stats:
        for path, rows in parquet_files:
            all_results.append((path, rows))
        if fjall_stats is not None:
            variants, positions, total_bytes, secs = fjall_stats
            log.info(
                "%s fjall: %d variants, %d positions, %.1f MB in %.1fs",
                entity_name,
                variants,
                positions,
                total_bytes / (1024 * 1024),
                secs,
            )

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
    skip_csq: bool = True,
    # Output mode
    output_vcf: str | None = None,
    show_progress: bool = True,
    compression: str | None = None,
    on_batch_written: "Callable[[int, int, int], None] | None" = None,
) -> "pl.LazyFrame | str":
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
    use_fjall : bool
        Use fjall (embedded KV store) backend instead of parquet
        (default: False).
    cache_size_mb : int
        Annotation cache size in MB (default: 1024).
    skip_csq : bool
        Exclude the raw CSQ column from the output (default: True).
        When True, only the parsed annotation columns are returned.
    output_vcf : str or None
        Path to write annotated VCF output. When set, annotation results are
        written directly to a VCF file and the output path is returned.
        When ``None`` (default), returns a polars ``LazyFrame``.
        Compression is auto-detected from the file extension: ``.vcf`` for
        plain text, ``.vcf.gz`` or ``.vcf.bgz`` for block-gzipped (bgzf).
        Override with the ``compression`` parameter.
    show_progress : bool
        Show a progress bar on stderr during VCF output (default: True).
        Only used when ``output_vcf`` is set.
    compression : str or None
        VCF output compression. ``"bgzf"`` (block-gzip, tabix-compatible),
        ``"gzip"``, ``"plain"``, or ``None`` (auto-detect from extension).
        Only used when ``output_vcf`` is set.
    on_batch_written : callable or None
        Callback invoked after each batch is written to VCF, with signature
        ``(batch_rows: int, total_rows: int, total_input: int)``.
        ``total_rows`` is the cumulative number of VCF records written so far.
        ``total_input`` is the total number of input variants when known.
        Useful for driving tqdm progress bars in notebooks. Only used when
        ``output_vcf`` is set.

    Returns
    -------
    polars.LazyFrame or str
        When ``output_vcf`` is ``None``: annotated variants as a polars
        ``LazyFrame`` with typed annotation columns plus original VCF fields.
        When ``output_vcf`` is set: the output VCF file path.

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

    >>> # Write annotated VCF directly
    >>> path = vepyr.annotate(
    ...     "input.vcf",
    ...     "/data/vep/parquet/115_GRCh38_vep",
    ...     everything=True,
    ...     reference_fasta="/ref/GRCh38.fa",
    ...     output_vcf="annotated.vcf",
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

    # VCF output path: write directly and return the path.
    if output_vcf is not None:
        if compression is not None:
            comp = compression
        elif output_vcf.endswith((".gz", ".bgz", ".bgzf")):
            comp = "bgzf"
        else:
            comp = "plain"

        # Build progress callback: explicit callback wins, then auto-tqdm
        # when show_progress=True, otherwise no callback.
        callback = on_batch_written
        _pbar = None
        if callback is None and show_progress:
            try:
                from tqdm.auto import tqdm

                _pbar = tqdm(
                    unit=" variants",
                    desc=f"Annotating → {output_vcf.rsplit('/', 1)[-1]}",
                    miniters=1,
                    mininterval=0,
                )

                def callback(batch_rows, total_rows, total_input):
                    if total_input > 0 and _pbar.total != total_input:
                        _pbar.total = total_input
                    _pbar.update(batch_rows)
                    _pbar.refresh()
            except ImportError:
                pass

        try:
            rows = _annotate_vcf(
                vcf,
                cache_dir,
                output_vcf,
                options_json,
                False,
                comp,
                callback,
            )
        finally:
            if _pbar is not None:
                _pbar.close()

        log.info("Wrote %d rows to %s", rows, output_vcf)
        return output_vcf

    import polars as pl
    import pyarrow as pa

    # Get schema from a probe annotator (doesn't consume data)
    probe = _create_annotator(vcf, cache_dir, options_json, skip_csq)
    pa_schema = probe.schema
    empty = pa.table({field.name: pa.array([], type=field.type) for field in pa_schema})
    polars_schema = dict(pl.from_arrow(empty).schema)
    del probe

    # Each collect() creates a fresh streaming annotator so the LazyFrame
    # is re-runnable (not single-use). Captures vcf/cache_dir/options by value.
    _vcf, _cache_dir, _opts, _skip = vcf, cache_dir, options_json, skip_csq

    def _batch_source(with_columns, predicate, n_rows, batch_size):
        # Pass n_rows as LIMIT to the DataFusion query for engine-level pushdown
        annotator = _create_annotator(_vcf, _cache_dir, _opts, _skip, n_rows)
        remaining = n_rows
        for py_batch in annotator:
            batch_df = pl.from_arrow(py_batch)
            if predicate is not None:
                batch_df = batch_df.filter(predicate)
            if with_columns is not None:
                batch_df = batch_df.select(with_columns)
            if remaining is not None:
                batch_df = batch_df.head(remaining)
                remaining -= batch_df.height
            if batch_df.height > 0:
                yield batch_df
            if remaining is not None and remaining <= 0:
                break

    from polars.io.plugins import register_io_source

    return register_io_source(
        io_source=_batch_source,
        schema=polars_schema,
    )
