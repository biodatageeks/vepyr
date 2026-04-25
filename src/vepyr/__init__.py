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


_DOWNLOAD_TIMEOUT = 300
_DOWNLOAD_MAX_RETRIES = 10
_DOWNLOAD_RETRY_BACKOFF = 5  # seconds, doubled each retry


def _download_with_progress(
    url: str, dest: str, _redirects: int = 0, max_retries: int = _DOWNLOAD_MAX_RETRIES
) -> None:
    """Download a file with a tqdm progress bar and resume-on-failure.

    On timeout or connection errors the download resumes from the last byte
    written using an HTTP Range header. Retries up to ``_DOWNLOAD_MAX_RETRIES``
    times with exponential backoff.
    """
    import http.client
    import os
    import time
    import urllib.parse

    from tqdm import tqdm

    filename = dest.rsplit("/", 1)[-1]
    log.info("Downloading %s", url)

    # --- Resolve redirects first so retries hit the final URL. ---
    parsed = urllib.parse.urlparse(url)
    conn = http.client.HTTPSConnection(parsed.hostname, timeout=_DOWNLOAD_TIMEOUT)
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
            return _download_with_progress(location, dest, _redirects + 1, max_retries)

    if resp.status != 200:
        conn.close()
        import urllib.error

        raise urllib.error.HTTPError(url, resp.status, resp.reason, resp.headers, None)

    total = int(resp.getheader("Content-Length", 0)) or None

    # --- Download with retry + resume ---
    downloaded = 0
    retries = 0
    backoff = _DOWNLOAD_RETRY_BACKOFF

    pbar = tqdm(
        total=total,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        desc=f"Downloading {filename}",
        miniters=1,
    )

    try:
        # First pass: use the already-open response.
        try:
            with open(dest, "wb") as f:
                while True:
                    buf = resp.read(8 * 1024 * 1024)
                    if not buf:
                        break
                    f.write(buf)
                    downloaded += len(buf)
                    pbar.update(len(buf))
            conn.close()
        except (TimeoutError, OSError, http.client.HTTPException):
            try:
                conn.close()
            except Exception:
                pass

        # If we got everything, we're done.
        if total is not None and downloaded >= total:
            return

        # No Content-Length but stream ended — assume complete.
        if total is None:
            return

        # --- Resume loop for incomplete downloads ---
        while downloaded < total:
            retries += 1
            if retries > max_retries:
                raise RuntimeError(
                    f"Download failed after {max_retries} retries "
                    f"({downloaded:,}/{total:,} bytes): {url}"
                )

            log.warning(
                "Download interrupted at %s/%s bytes, retrying in %ds (attempt %d/%d)",
                f"{downloaded:,}",
                f"{total:,}",
                backoff,
                retries,
                max_retries,
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, 120)

            try:
                parsed = urllib.parse.urlparse(url)
                conn = http.client.HTTPSConnection(
                    parsed.hostname, timeout=_DOWNLOAD_TIMEOUT
                )
                conn.request(
                    "GET",
                    parsed.path,
                    headers={
                        "Accept-Encoding": "identity",
                        "Range": f"bytes={downloaded}-",
                    },
                )
                resp = conn.getresponse()

                if resp.status not in (200, 206):
                    conn.close()
                    continue

                # If server ignores Range and sends 200, restart from scratch.
                if resp.status == 200:
                    downloaded = 0
                    pbar.reset()
                    mode = "wb"
                else:
                    mode = "ab"

                with open(dest, mode) as f:
                    while True:
                        buf = resp.read(8 * 1024 * 1024)
                        if not buf:
                            break
                        f.write(buf)
                        downloaded += len(buf)
                        pbar.update(len(buf))
                conn.close()
            except (TimeoutError, OSError, http.client.HTTPException) as exc:
                log.debug("Retry %d failed: %s", retries, exc)
                continue
    finally:
        pbar.close()

    # Verify final size.
    actual = os.path.getsize(dest)
    if total is not None and actual != total:
        raise RuntimeError(
            f"Download size mismatch: expected {total:,} bytes, got {actual:,}"
        )


def _download_cache(
    release: int,
    species: str,
    assembly: str,
    cache_type: str,
    dest: str,
    max_retries: int = _DOWNLOAD_MAX_RETRIES,
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
            _download_with_progress(url, dest, max_retries=max_retries)
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
    download_retries: int = 10,
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
    download_retries : int
        Maximum number of resume-retries for the cache download (default: 10).
        Each retry resumes from the last byte received.
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
    if not 1 <= fjall_zstd_level <= 22:
        raise ValueError(
            f"fjall_zstd_level must be between 1 and 22, got {fjall_zstd_level}"
        )
    if fjall_dict_size_kb < 0:
        raise ValueError(
            f"fjall_dict_size_kb must be non-negative, got {fjall_dict_size_kb}"
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
        cache_root = os.path.join(
            cache_dir, species, f"{release}_{assembly}{method_infix}"
        )

        os.makedirs(cache_dir, exist_ok=True)

        if not os.path.isdir(cache_root):
            if not os.path.isfile(tarball_path):
                _download_cache(
                    release,
                    species,
                    assembly,
                    cache_type,
                    tarball_path,
                    max_retries=download_retries,
                )

            tarball_size_mb = os.path.getsize(tarball_path) / (1024 * 1024)
            log.info("Extracting %s (%.0f MB) ...", tarball_name, tarball_size_mb)
            with tarfile.open(tarball_path) as tar:
                tar.extractall(path=cache_dir, filter="data")
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

    # When using multiple partitions, skip the Python progress callback to avoid
    # GIL contention — each tokio worker would re-acquire the GIL per batch,
    # serializing the parallel work.
    if on_progress is not None and partitions > 1:
        import warnings

        warnings.warn(
            "on_progress callback is disabled when partitions > 1 to avoid GIL contention.",
            stacklevel=2,
        )
    native_cb = progress_cb if partitions <= 1 else None

    try:
        entity_stats = _build_cache(
            cache_root,
            output_dir,
            partitions,
            build_fjall,
            fjall_zstd_level,
            fjall_dict_size_kb,
            native_cb,
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
    refseq: bool = False,
    gencode_basic: bool = False,
    gencode_primary: bool = False,
    all_refseq: bool = False,
    exclude_predicted: bool = False,
    pick: bool = False,
    pick_allele: bool = False,
    per_gene: bool = False,
    pick_allele_gene: bool = False,
    flag_pick: bool = False,
    flag_pick_allele: bool = False,
    flag_pick_allele_gene: bool = False,
    pick_order: str | None = None,
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
        Use merged Ensembl+RefSeq cache. Adds ``SOURCE``, ``REFSEQ_MATCH``,
        ``REFSEQ_OFFSET``, ``GIVEN_REF``, ``USED_REF``, ``BAM_EDIT`` CSQ
        fields. Mutually exclusive with ``refseq``.
    refseq : bool
        Use RefSeq cache/transcripts instead of Ensembl. Adds
        ``REFSEQ_MATCH``, ``REFSEQ_OFFSET``, ``GIVEN_REF``, ``USED_REF``,
        ``BAM_EDIT`` CSQ fields. Mutually exclusive with ``merged``,
        ``gencode_basic``, and ``gencode_primary``.
    gencode_basic : bool
        Restrict to transcripts in the GENCODE basic set. Mutually exclusive
        with ``gencode_primary`` and ``refseq``.
    gencode_primary : bool
        Restrict to transcripts in the GENCODE primary set (GRCh38 only).
        Mutually exclusive with ``gencode_basic`` and ``refseq``.
    all_refseq : bool
        Keep all RefSeq transcripts including CCDS/EST-style rows.
    exclude_predicted : bool
        Exclude predicted RefSeq transcripts (``XM_`` / ``XR_`` prefixes).
    pick : bool
        Emit one selected consequence per variant, matching VEP ``--pick``.
    pick_allele : bool
        Emit one selected consequence per allele, matching VEP
        ``--pick_allele``.
    per_gene : bool
        Emit one selected consequence per gene while retaining non-transcript
        rows, matching VEP ``--per_gene``.
    pick_allele_gene : bool
        Emit one selected consequence per allele and gene, matching VEP
        ``--pick_allele_gene``.
    flag_pick : bool
        Retain all consequences and add ``PICK=1`` to one selected entry per
        variant, matching VEP ``--flag_pick``.
    flag_pick_allele : bool
        Retain all consequences and add ``PICK=1`` to one selected entry per
        allele, matching VEP ``--flag_pick_allele``.
    flag_pick_allele_gene : bool
        Add a standalone ``PICK=1`` CSQ field for the selected transcript per
        allele and gene, matching VEP ``--flag_pick_allele_gene``.
    pick_order : str or None
        Comma-separated VEP pick ranking order, e.g.
        ``"biotype,rank,mane_select,tsl,canonical,appris,ccds,length"``.
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
    if (everything or hgvs or hgvsc or hgvsp) and not reference_fasta:
        raise ValueError(
            "reference_fasta is required when everything/hgvs/hgvsc/hgvsp=True"
        )

    # Validate mutual exclusivity of cache/transcript flags
    if refseq and merged:
        raise ValueError("refseq and merged are mutually exclusive")
    if refseq and (gencode_basic or gencode_primary):
        raise ValueError(
            "refseq is mutually exclusive with gencode_basic and gencode_primary"
        )
    if gencode_basic and gencode_primary:
        raise ValueError("gencode_basic and gencode_primary are mutually exclusive")

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
    if refseq:
        opts["refseq"] = True
    if gencode_basic:
        opts["gencode_basic"] = True
    if gencode_primary:
        opts["gencode_primary"] = True
    if all_refseq:
        opts["all_refseq"] = True
    if exclude_predicted:
        opts["exclude_predicted"] = True
    for key, enabled in {
        "pick": pick,
        "pick_allele": pick_allele,
        "per_gene": per_gene,
        "pick_allele_gene": pick_allele_gene,
        "flag_pick": flag_pick,
        "flag_pick_allele": flag_pick_allele,
        "flag_pick_allele_gene": flag_pick_allele_gene,
    }.items():
        if enabled:
            opts[key] = True
    if pick_order:
        opts["pick_order"] = pick_order
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
        _pending_updates = None
        if callback is None and show_progress:
            try:
                import queue

                from tqdm.auto import tqdm

                _pbar = tqdm(
                    unit=" variants",
                    desc=f"Annotating → {output_vcf.rsplit('/', 1)[-1]}",
                    miniters=1,
                    mininterval=0,
                )
                _pending_updates = queue.SimpleQueue()

                def callback(batch_rows, total_rows, total_input):
                    _pending_updates.put((batch_rows, total_rows, total_input))
            except ImportError:
                pass

        try:
            # Run the native call in a background thread so Jupyter's event
            # loop can pump display updates (tqdm progress) while the Rust
            # side streams batches.  The native code releases the GIL via
            # py.allow_threads(), so the main thread stays responsive.
            import threading

            _result: list = [None]
            _error: list = [None]

            def _run() -> None:
                try:
                    _result[0] = _annotate_vcf(
                        vcf,
                        cache_dir,
                        output_vcf,
                        options_json,
                        False,
                        comp,
                        callback,
                    )
                except Exception as exc:
                    _error[0] = exc

            def _drain_progress_updates() -> None:
                if _pbar is None or _pending_updates is None:
                    return
                while True:
                    try:
                        batch_rows, _total_rows, total_input = (
                            _pending_updates.get_nowait()
                        )
                    except queue.Empty:
                        break
                    if total_input > 0 and _pbar.total != total_input:
                        _pbar.total = total_input
                    _pbar.update(batch_rows)
                    _pbar.refresh()

            t = threading.Thread(target=_run, daemon=True)
            t.start()
            while t.is_alive():
                t.join(timeout=0.3)
                _drain_progress_updates()
            _drain_progress_updates()
            if _error[0] is not None:
                raise _error[0]
            rows = _result[0]
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
