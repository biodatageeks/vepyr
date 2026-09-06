from __future__ import annotations

import contextlib
import importlib.metadata
import logging
import os
import re
import warnings
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl

from vepyr._core import annotate_vcf as _annotate_vcf
from vepyr._core import build_cache as _build_cache
from vepyr._core import build_cache_entity as _build_cache_entity
from vepyr._core import build_plugin_cache as _build_plugin_cache
from vepyr._core import cache_contig_identity_json as _cache_contig_identity_json
from vepyr._core import create_annotator as _create_annotator
from vepyr._core import supported_vep_targets_json as _supported_vep_targets_json
from vepyr._core import vcf_contigs as _vcf_contigs
from vepyr._regions import GENOMIC_COLUMNS, extract_regions

__all__ = [
    "annotate",
    "build_cache",
    "build_cache_entity",
    "build_plugin_cache",
    "cache_contig_identity",
    "supported_vep_targets",
]

__version__ = importlib.metadata.version("vepyr")

log = logging.getLogger(__name__)

# Projection pushdown: which annotation flags each DataFrame column depends on.
# Every other column has the same value whatever flags are set, so a
# ``select()`` decides which groups the engine runs: unused groups are dropped
# and, when no flag was given, needed groups are switched on. Verified column by column on HG002 chr22 against the
# release-116 Ensembl cache; ``tests/test_annotate.py::TestProjectionPruning``
# guards the value identity on the fixture cache.
_HGVS_COLUMNS = frozenset({"HGVSc", "HGVSp"})
_HGVS_OPTIONS = (
    "hgvs",
    "hgvsc",
    "hgvsp",
    "shift_hgvs",
    "no_escape",
    "remove_hgvsp_version",
    "hgvsp_use_prediction",
)
_COLOCATED_COLUMNS = frozenset(
    {
        "Existing_variation",
        "AF",
        "AFR_AF",
        "AMR_AF",
        "EAS_AF",
        "EUR_AF",
        "SAS_AF",
        "gnomADe_AF",
        "gnomADg_AF",
        "MAX_AF",
        "MAX_AF_POPS",
        "CLIN_SIG",
        "SOMATIC",
        "PHENO",
        "PUBMED",
        # cache-only columns: kept conservative, they come from the same lookup
        "clin_sig_allele",
        "clinical_impact",
        "minor_allele",
        "minor_allele_freq",
        "clinvar_ids",
        "cosmic_ids",
        "dbsnp_ids",
    }
)
_COLOCATED_OPTIONS = (
    "check_existing",
    "af",
    "af_1kg",
    "af_gnomade",
    "af_gnomadg",
    "max_af",
    "pubmed",
)
# Columns only ``everything`` fills; selecting one keeps the flag as is. The
# motif columns are among them because the five motif fields exist only in
# the ``everything`` CSQ layout (the engine leaves them null otherwise).
_EVERYTHING_ONLY_COLUMNS = frozenset(
    {
        "MOTIF_NAME",
        "MOTIF_POS",
        "HIGH_INF_POS",
        "MOTIF_SCORE_CHANGE",
        "TRANSCRIPTION_FACTORS",
        "MANE",
        "APPRIS",
        "SIFT",
        "PolyPhen",
        "DOMAINS",
        "miRNA",
        "HGVS_OFFSET",
        "gnomADe_AFR_AF",
        "gnomADe_AMR_AF",
        "gnomADe_ASJ_AF",
        "gnomADe_EAS_AF",
        "gnomADe_FIN_AF",
        "gnomADe_MID_AF",
        "gnomADe_NFE_AF",
        "gnomADe_REMAINING_AF",
        "gnomADe_SAS_AF",
        "gnomADg_AFR_AF",
        "gnomADg_AMI_AF",
        "gnomADg_AMR_AF",
        "gnomADg_ASJ_AF",
        "gnomADg_EAS_AF",
        "gnomADg_FIN_AF",
        "gnomADg_MID_AF",
        "gnomADg_NFE_AF",
        "gnomADg_REMAINING_AF",
        "gnomADg_SAS_AF",
    }
)


# Plugin cache manifest value types (engine ``ValueType``) to Polars dtypes.
_PLUGIN_VALUE_TYPES = {"Utf8": "String", "Float32": "Float32", "Int32": "Int32"}


def _plugin_value_dtype(type_name: str | None):
    import polars as pl

    return getattr(pl, _PLUGIN_VALUE_TYPES.get(type_name, "String"))


def _plugin_column(values, dtype, per_variant: bool):
    """Shape one plugin field parsed out of CSQ: ``values`` is a list of one
    string per consequence entry. A per-variant plugin repeats the same value
    on every entry, so it collapses to one typed scalar; a per-feature plugin
    stays a typed list aligned with ``Consequence``."""
    import polars as pl

    if per_variant:
        return values.list.drop_nulls().list.first().cast(dtype)
    return values.cast(pl.List(dtype))


def _flags_for_projection(
    opts: dict,
    needed: set[str] | None,
    available: set[str] | None = None,
    required: frozenset[str] | set[str] = frozenset(),
) -> dict:
    """Derive the annotation flags a query needs from the columns it reads.

    ``needed`` is the query's projection plus any filter columns. With no
    projection (``None``), or when the raw ``CSQ`` string is read, the flags
    stay as given, or, when none were given, ``everything`` is used with a
    FASTA and the co-located lookup without one. Otherwise only three column
    groups depend on flags at all (see the constants above):

    - a group nobody selected has its flags removed, so the engine skips it;
    - a group the user enabled explicitly is kept exactly as configured;
    - a group the user did not mention is enabled when a column needs it.
      HGVS and the ``everything`` extras need ``reference_fasta``; asking for
      them without one is an error rather than a column of nulls.

    ``available`` is the frame's column set; it limits the no-FASTA warning
    to columns the frame has. ``required`` names fields that must be computed
    whatever the projection, the fields a plugin's match templates read: their
    groups are switched on even when other flags were given explicitly.
    Returns a new dict.
    """
    out = dict(opts)
    user_hgvs = any(opts.get(key) for key in ("hgvs", "hgvsc", "hgvsp"))
    user_colocated = any(opts.get(key) for key in _COLOCATED_OPTIONS)
    user_any_flag = bool(opts.get("everything")) or user_hgvs or user_colocated

    def _require_fasta(group: frozenset, flag: str, fields: set[str]) -> None:
        if not out.get("reference_fasta_path"):
            columns = ", ".join(sorted(fields & group))
            raise ValueError(
                f"selecting {columns} needs {flag}, which requires reference_fasta="
            )

    def _ensure(fields: set[str]) -> None:
        """Switch on the groups ``fields`` need, whatever the user set."""
        if fields & _EVERYTHING_ONLY_COLUMNS and not out.get("everything"):
            _require_fasta(_EVERYTHING_ONLY_COLUMNS, "everything", fields)
            out["everything"] = True
        if out.get("everything"):
            return
        # hgvs computes both HGVS fields; hgvsc and hgvsp one each, so the
        # check is per field: hgvsp=True alone leaves HGVSc empty. With no
        # HGVS flag at all, hgvs is switched on like the projection does.
        if fields & _HGVS_COLUMNS and not any(
            out.get(key) for key in ("hgvs", "hgvsc", "hgvsp")
        ):
            _require_fasta(_HGVS_COLUMNS, "hgvs", fields)
            out["hgvs"] = True
        for field, flag in (("HGVSc", "hgvsc"), ("HGVSp", "hgvsp")):
            if field in fields and not (out.get("hgvs") or out.get(flag)):
                _require_fasta(_HGVS_COLUMNS, flag, {field})
                out[flag] = True
        if fields & _COLOCATED_COLUMNS and not any(
            out.get(key) for key in _COLOCATED_OPTIONS
        ):
            for key in _COLOCATED_OPTIONS:
                out[key] = True

    if needed is None or "CSQ" in needed:
        # No projection, or the raw CSQ string (which needs every flag): flags
        # as given, or, when none were given, everything the inputs allow.
        # HGVS and the everything extras need a FASTA, so without one only
        # the co-located lookup can be switched on. Plugin requirements come
        # first so a missing FASTA raises before anything is warned about.
        _ensure(set(required))
        if not user_any_flag:
            if out.get("reference_fasta_path"):
                out["everything"] = True
            else:
                for key in _COLOCATED_OPTIONS:
                    out[key] = True
                unavailable = _HGVS_COLUMNS | _EVERYTHING_ONLY_COLUMNS
                if available is not None:
                    unavailable = unavailable & available
                if unavailable:
                    warnings.warn(
                        "no reference_fasta given, so these columns will be null: "
                        f"{', '.join(sorted(unavailable))}. Pass reference_fasta= "
                        "for the full result",
                        stacklevel=2,
                    )
        return out

    needed = needed | set(required)

    def _needs(group: frozenset) -> bool:
        return bool(needed & group)

    if _needs(_EVERYTHING_ONLY_COLUMNS):
        if not opts.get("everything"):
            _require_fasta(_EVERYTHING_ONLY_COLUMNS, "everything", needed)
            out["everything"] = True
        return out  # everything covers every group; sub-options stay as given

    keep_hgvs = _needs(_HGVS_COLUMNS)
    keep_colocated = _needs(_COLOCATED_COLUMNS)
    if out.pop("everything", False):
        # Expand into the groups still needed; each alone yields the same
        # column values as ``everything`` does.
        if keep_hgvs:
            out["hgvs"] = True
        if keep_colocated:
            for key in _COLOCATED_OPTIONS:
                out[key] = True
    else:
        if keep_hgvs and not user_hgvs:
            _require_fasta(_HGVS_COLUMNS, "hgvs", needed)
            out["hgvs"] = True
        if keep_colocated and not user_colocated:
            for key in _COLOCATED_OPTIONS:
                out[key] = True
    if not keep_hgvs:
        for key in _HGVS_OPTIONS:
            out.pop(key, None)
    if not keep_colocated:
        for key in _COLOCATED_OPTIONS:
            out.pop(key, None)
    # Plugin template fields are checked per field: hgvsp=True alone does not
    # compute the HGVSc a template may read.
    _ensure(set(required))
    if not any(out.get(key) for key in ("hgvs", "hgvsc", "hgvsp")):
        out.pop("reference_fasta_path", None)
    return out


_CORE_CSQ_FIELDS = (
    "Allele",
    "Gene",
    "Feature",
    "Feature_type",
    "Consequence",
    "cDNA_position",
    "CDS_position",
    "Protein_position",
    "Amino_acids",
    "Codons",
    "Existing_variation",
)

# Ensembl FTP URL templates for VEP cache tarballs.
# {method_infix} is "" for Ensembl, "_merged" for merged, "_refseq" for RefSeq.
# Release >=115 uses indexed_vep_cache/, older releases use vep/.
_ENSEMBL_FTP_PATHS = [
    "https://ftp.ensembl.org/pub/release-{release}/variation/indexed_vep_cache/{species}{method_infix}_vep_{release}_{assembly}.tar.gz",
    "https://ftp.ensembl.org/pub/release-{release}/variation/vep/{species}{method_infix}_vep_{release}_{assembly}.tar.gz",
]


_MAX_REDIRECTS = 5


_DOWNLOAD_TIMEOUT = 300
_DOWNLOAD_MAX_RETRIES = 10
_DOWNLOAD_RETRY_BACKOFF = 5  # seconds, doubled each retry

_CACHE_TYPE_TO_DOWNLOAD_INFIX = {
    "ensembl": "",
    "merged": "_merged",
    "refseq": "_refseq",
}
_PUBLIC_CACHE_TYPES = ("ensembl", "merged", "refseq")
_PUBLIC_CACHE_ENTITIES = (
    "variation",
    "transcript",
    "exon",
    "translation",
    "regulatory",
    "motif",
)


def supported_vep_targets() -> tuple[dict[str, str], ...]:
    """Return the annotation engine's compiled VEP/cache compatibility records."""
    import json

    records = json.loads(_supported_vep_targets_json())
    return tuple(dict(record) for record in records)


def cache_contig_identity(
    cache_dir: str,
    chrom: str,
    *,
    expected_cache_version: str | None = None,
) -> dict[str, str]:
    """Validate and return Parquet metadata for one cache contig.

    Only shards selected for ``chrom`` are opened. The optional expected value
    is an assertion and cannot substitute for missing shard metadata.
    """
    import json

    _validate_expected_cache_version(expected_cache_version)
    return dict(
        json.loads(
            _cache_contig_identity_json(
                cache_dir,
                chrom,
                expected_cache_version,
            )
        )
    )


def _validate_expected_cache_version(value: str | None) -> None:
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, str):
        raise TypeError("expected_cache_version must be a string or None")
    supported = {target["cache_version"] for target in supported_vep_targets()}
    if value not in supported:
        raise ValueError(
            f"Unsupported expected_cache_version {value!r}; "
            f"supported cache versions: {', '.join(sorted(supported))}"
        )


def _cache_version_for_release(release: int) -> str:
    if isinstance(release, bool) or not isinstance(release, int):
        raise TypeError("release must be an integer Ensembl cache release")
    cache_version = str(release)
    _validate_expected_cache_version(cache_version)
    return cache_version


def _validate_cache_type(cache_type: str) -> None:
    if cache_type in _CACHE_TYPE_TO_DOWNLOAD_INFIX:
        return

    allowed = "', '".join(_PUBLIC_CACHE_TYPES)
    raise ValueError(f"Invalid cache_type '{cache_type}'. Must be one of '{allowed}'.")


def _validate_cache_entity(entity: str) -> None:
    if entity in _PUBLIC_CACHE_ENTITIES:
        return

    allowed = "', '".join(_PUBLIC_CACHE_ENTITIES)
    raise ValueError(f"Invalid cache entity '{entity}'. Must be one of '{allowed}'.")


def _download_with_progress(
    url: str, dest: str, _redirects: int = 0, max_retries: int = _DOWNLOAD_MAX_RETRIES
) -> None:
    """Download a file with a tqdm progress bar and resume-on-failure.

    On timeout or connection errors the download resumes from the last byte
    written using an HTTP Range header. Retries up to ``_DOWNLOAD_MAX_RETRIES``
    times with exponential backoff.
    """
    import http.client
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

    method_infix = _CACHE_TYPE_TO_DOWNLOAD_INFIX[cache_type]

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


def _resolve_raw_cache(
    release: int,
    cache_dir: str,
    *,
    cache_type: str,
    species: str,
    assembly: str,
    local_cache: str | None,
    download_retries: int,
) -> str:
    """Return an unpacked raw cache, downloading and extracting it if needed."""
    import tarfile

    if local_cache is not None:
        if not os.path.isdir(local_cache):
            raise FileNotFoundError(f"Local cache directory not found: {local_cache}")
        log.info("Using local cache: %s", local_cache)
        return local_cache

    method_infix = _CACHE_TYPE_TO_DOWNLOAD_INFIX[cache_type]
    tarball_name = f"{species}{method_infix}_vep_{release}_{assembly}.tar.gz"
    tarball_path = os.path.join(cache_dir, tarball_name)
    cache_root = os.path.join(
        cache_dir,
        f"{species}{method_infix}",
        f"{release}_{assembly}",
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

    return cache_root


def build_cache(
    release: int,
    cache_dir: str,
    *,
    cache_type: str,
    species: str = "homo_sapiens",
    assembly: str = "GRCh38",
    partitions: int = 8,
    cache_format: str = "parquet",
    local_cache: str | None = None,
    download_retries: int = 10,
    show_progress: bool = True,
    on_progress: Callable[[str, str, int, int, int], None] | None = None,
    overwrite: bool = False,
) -> list[tuple[str, int]]:
    """Download an Ensembl VEP cache and convert it to an optimized cache.

    Parameters
    ----------
    release : int
        Ensembl release number (e.g. 115).
    cache_dir : str
        Root directory for cache data and Parquet output.
    cache_type : str
        Required Ensembl VEP cache type: ``"ensembl"``, ``"merged"``, or
        ``"refseq"``.
    species : str
        Species name (default: ``"homo_sapiens"``).
    assembly : str
        Genome assembly (default: ``"GRCh38"``).
    partitions : int
        Number of DataFusion partitions for parallelism (default: 8).
    cache_format : str
        Cache format to build. Only ``"parquet"`` is supported (default).
    local_cache : str or None
        Path to an already-unpacked Ensembl VEP cache directory (the one
        containing ``info.txt``). When provided, downloading and extraction
        are skipped entirely.
    download_retries : int
        Maximum number of resume-retries for the cache download (default: 10).
        Each retry resumes from the last byte received.
    show_progress : bool
        Show tqdm progress bars during conversion (default: True).

        .. note::
           The partitioned-Parquet build path does not currently emit
           per-batch progress events, so no bars appear during the cache
           build regardless of ``show_progress`` / ``on_progress``.
    on_progress : callable or None
        Custom progress callback with signature
        ``(entity, format, batch_rows, total_rows, total_expected)``.
        Overrides the default tqdm bars when provided. See the note on
        ``show_progress`` — the Parquet build path does not invoke it.
    overwrite : bool
        Rebuild existing cache outputs instead of skipping them.

    Returns
    -------
    list[tuple[str, int]]
        List of ``(parquet_file_path, row_count)`` for each written file.
    """

    _validate_cache_type(cache_type)
    expected_cache_version = _cache_version_for_release(release)
    if cache_format != "parquet":
        raise ValueError("cache_format must be 'parquet'")

    # Version directory name: e.g. "115_GRCh38_ensembl"
    version_dir = f"{release}_{assembly}_{cache_type}"
    cache_root = _resolve_raw_cache(
        release,
        cache_dir,
        cache_type=cache_type,
        species=species,
        assembly=assembly,
        local_cache=local_cache,
        download_retries=download_retries,
    )

    # Output layout (parquet): <version_dir>/<entity>.parquet/chr1.parquet
    output_dir = os.path.join(cache_dir, version_dir)

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
            cache_format,
            native_cb,
            cache_type,
            overwrite,
            expected_cache_version,
        )
    finally:
        if _bars is not None:
            for bar in _bars.values():
                bar.close()

    # Flatten entity stats into the simple (path, rows) list for backward compat
    all_results: list[tuple[str, int]] = []
    for _entity_name, parquet_files, _legacy_stats in entity_stats:
        for path, rows in parquet_files:
            all_results.append((path, rows))

    log.info("Done. Wrote %d Parquet datasets to %s", len(all_results), output_dir)
    return all_results


def build_cache_entity(
    release: int,
    cache_dir: str,
    entity: str,
    *,
    cache_type: str,
    species: str = "homo_sapiens",
    assembly: str = "GRCh38",
    partitions: int = 8,
    local_cache: str | None = None,
    download_retries: int = 10,
    overwrite: bool = False,
    chroms: list[str] | None = None,
) -> list[tuple[str, int]]:
    """Download or open an Ensembl VEP cache and convert one raw entity.

    This is the targeted counterpart to :func:`build_cache`. It applies the
    same exact release/source validation and writes into the same
    ``<release>_<assembly>_<cache_type>`` output directory. ``entity`` must be
    one of ``variation``, ``transcript``, ``exon``, ``translation``,
    ``regulatory``, or ``motif``. The raw ``translation`` entity produces the
    ``translation_core`` and ``translation_sift`` Parquet datasets.

    ``chroms`` restricts the rebuild to specific contigs (e.g. ``["chrX"]``);
    ``None`` rebuilds every contig.

    Returns a flattened list of ``(parquet_file_path, row_count)`` pairs.
    """

    _validate_cache_type(cache_type)
    _validate_cache_entity(entity)
    expected_cache_version = _cache_version_for_release(release)
    cache_root = _resolve_raw_cache(
        release,
        cache_dir,
        cache_type=cache_type,
        species=species,
        assembly=assembly,
        local_cache=local_cache,
        download_retries=download_retries,
    )
    output_dir = os.path.join(cache_dir, f"{release}_{assembly}_{cache_type}")

    entity_stats = _build_cache_entity(
        cache_root,
        output_dir,
        entity,
        partitions,
        cache_type,
        overwrite,
        expected_cache_version,
        chroms,
    )

    results = [
        (path, rows)
        for _entity_name, parquet_files, _legacy_stats in entity_stats
        for path, rows in parquet_files
    ]
    log.info(
        "Done. Wrote %d Parquet datasets for %s to %s",
        len(results),
        entity,
        output_dir,
    )
    return results


DEFAULT_PLUGINS_REPO_URL = "https://github.com/biodatageeks/vepyr-plugins.git"


@contextlib.contextmanager
def _resolve_plugin_manifest(
    plugin: str,
    version: str,
    *,
    plugins_repo: str | None = None,
    repo_url: str = DEFAULT_PLUGINS_REPO_URL,
) -> Iterator[tuple[str, str]]:
    """Resolve ``plugins/<plugin>/<plugin>.source.toml`` at git tag ``version``.

    Offline: reuse a provided local clone (``plugins_repo``). Online: clone the
    public repo into a temp dir. Either way, materialize the file at ``version``
    via ``git worktree`` (never disturbs the caller's checkout).

    Yields the manifest path and the immutable commit SHA to which ``version``
    resolved. A context manager: the temp clone (online only) and the worktree —
    including its registration in the source repo's ``.git/worktrees/`` — are
    removed on exit, so repeated builds don't leak ``/tmp`` clones or stale
    worktree entries.
    """
    import shutil
    import subprocess
    import tempfile

    created_clone = plugins_repo is None
    repo = plugins_repo
    worktree = None
    try:
        # Create the temp clone INSIDE the try so a failed `git clone` (bad url,
        # network, credentials) still hits the cleanup below and doesn't leave a
        # stale `vepyr-plugins-*` dir behind.
        if created_clone:
            repo = tempfile.mkdtemp(prefix="vepyr-plugins-")
            subprocess.run(["git", "clone", "--quiet", repo_url, repo], check=True)
        resolved_ref = subprocess.run(
            [
                "git",
                "-C",
                repo,
                "rev-parse",
                "--verify",
                "--quiet",
                f"{version}^{{commit}}",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if resolved_ref.returncode != 0:
            resolved_ref = subprocess.run(
                [
                    "git",
                    "-C",
                    repo,
                    "rev-parse",
                    "--verify",
                    "--quiet",
                    f"origin/{version}^{{commit}}",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
        resolved_commit = resolved_ref.stdout.strip()
        worktree = tempfile.mkdtemp(prefix="vepyr-plugins-wt-")
        subprocess.run(
            [
                "git",
                "-C",
                repo,
                "worktree",
                "add",
                "--quiet",
                "--detach",
                worktree,
                resolved_commit,
            ],
            check=True,
        )
        rel = os.path.join("plugins", plugin, f"{plugin}.source.toml")
        manifest = os.path.join(worktree, rel)
        if not os.path.exists(manifest):
            raise FileNotFoundError(f"{rel} not found at {version} in {repo}")
        yield manifest, resolved_commit
    finally:
        # Remove the worktree (deletes the dir AND its registration in `repo`);
        # `rmtree` is a belt-and-suspenders cleanup if `worktree remove` failed
        # (e.g. it never got added). Drop the whole temp clone we created — even
        # if it's a partial/failed clone (worktree is None in that case).
        if worktree is not None:
            subprocess.run(
                ["git", "-C", repo, "worktree", "remove", "--force", worktree],
                check=False,
                capture_output=True,
            )
            shutil.rmtree(worktree, ignore_errors=True)
        if created_clone and repo is not None:
            shutil.rmtree(repo, ignore_errors=True)


def build_plugin_cache(
    plugin: str,
    version: str,
    *,
    source_path: str | dict[str, str],
    cache_dir: str,
    plugin_cache_root: str,
    chroms: list[str] | None = None,
    plugins_repo: str | None = None,
    overwrite: bool = False,
    verify_source: bool | str = True,
) -> list[tuple[str, int, int, int]]:
    """Build a per-chromosome plugin cache.

    ``plugin``/``version`` select ``plugins/<plugin>/<plugin>.source.toml`` from
    the public vepyr-plugins repo at that git tag (or ``plugins_repo`` for
    offline). Tiering is inherited from the variation cache at ``cache_dir``.
    Returns per-chrom ``(chrom, rows, warm, cold)`` tuples.

    ``source_path`` points each ``[[source]]`` at a real file, since the paths a
    manifest ships are placeholders. A single-source manifest takes a plain path;
    a manifest that declares several sources (each with a ``part``) takes a
    ``{part: path}`` mapping, and every part must be mapped::

        build_plugin_cache(
            "cadd", "v1.0",
            source_path={"snv": ".../whole_genome_SNVs.tsv.gz",
                         "indel": ".../gnomad.genomes.r4.0.indel.tsv.gz"},
            ...
        )

    The sources are registered as ``plugin_<name>_src_<part>`` and combined by the
    manifest's own ``ingest_sql`` -- there is no need to concatenate them first.

    ``verify_source`` guards against building from the wrong bytes. Before the
    first chromosome is ingested, each resolved source file is MD5-hashed once
    (streaming, bounded memory) and compared with the manifest's ``md5``.
    ``True`` / ``"strict"`` (default) raises on a mismatch,
    naming the part, both digests and the upstream ``url``; ``"warn"`` logs it
    and keeps building -- for a build input that is a derived artifact of the
    upstream file (AlphaMissense's BGZF re-compression; its plugin README
    documents the preprocessing); ``False`` / ``"skip"`` never hashes -- use it
    for a chromosome slice cut with ``tabix``, whose digest can never match the
    whole file. A manifest that declares no ``md5`` is never hashed. The digest
    actually verified, with the file's size and mtime, is recorded under
    ``sources`` in the emitted ``manifest.json``; an incremental ``chroms=[...]``
    build against an unchanged file trusts that record instead of re-hashing.

    The cache manifest records the source as ``<version>@<commit SHA>``. This
    keeps mutable refs auditable and prevents an incremental build from mixing
    chromosomes produced from different manifest revisions.
    """
    mode = _normalize_verify_source(verify_source)
    with _resolve_plugin_manifest(plugin, version, plugins_repo=plugins_repo) as (
        manifest_path,
        resolved_commit,
    ):
        result, sources_json = _build_plugin_cache(
            manifest_path,
            source_path,
            cache_dir,
            plugin_cache_root,
            chroms,
            overwrite,
            mode,
            f"{version}@{resolved_commit}",
        )
    if mode == "warn":
        import json

        _warn_on_source_mismatch(plugin, json.loads(sources_json))
    return result


def _warn_on_source_mismatch(plugin: str, sources: list[dict]) -> None:
    """Raise a ``RuntimeWarning`` for every source a ``"warn"`` build accepted
    with a digest other than the manifest's.

    The engine logs the mismatch at warn level, but the native module's logger
    only shows errors unless ``RUST_LOG`` is set, so a Python caller would not
    see it. ``sources`` is the provenance this build recorded, returned by the
    native call rather than read back from the live manifest, which another
    writer may have replaced meanwhile.
    """
    for source in sources:
        declared, found = source.get("md5"), source.get("verified_md5")
        if not declared or not found or declared == found:
            continue
        part = f" part {source['part']!r}" if source.get("part") else ""
        message = (
            f"plugin {plugin!r}{part}: built from {source.get('file')!r} with MD5 "
            f"{found}, which differs from the manifest's {declared} "
            f"(verify_source='warn'); the cache manifest records the digest found"
        )
        log.warning(message)
        warnings.warn(message, RuntimeWarning, stacklevel=3)


_VERIFY_SOURCE_MODES = ("strict", "warn", "skip")


def _normalize_verify_source(verify_source: bool | str) -> str:
    """Map ``build_plugin_cache``'s ``verify_source`` onto the engine's mode."""
    if verify_source is True:
        return "strict"
    if verify_source is False:
        return "skip"
    if isinstance(verify_source, str) and verify_source in _VERIFY_SOURCE_MODES:
        return verify_source
    raise ValueError(
        f"verify_source must be True, False or one of {_VERIFY_SOURCE_MODES}, "
        f"got {verify_source!r}"
    )


def _require_index_for_workers(vcf: str, workers: int) -> None:
    """``workers>1`` reads each run's position window by index seek on both
    output paths; without an index every run would parse the whole file."""
    if workers <= 1:
        return
    if os.path.exists(vcf + ".tbi") or os.path.exists(vcf + ".csi"):
        return
    raise ValueError(
        f"workers>1 requires a tabix-indexed input ({vcf}.tbi or .csi); "
        "compress with bgzip and index with tabix, or use workers=1"
    )


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
    cache_format: str = "parquet",
    expected_cache_version: str | None = None,
    extended_probes: bool = True,
    distance: int | tuple[int, int] | None = None,
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
    buffer_size: int = 5000,
    failed: int = 0,
    # Engine tuning
    cache_size_mb: int = 1024,
    workers: int = 1,
    skip_csq: bool = True,
    fields: str | list[str] | tuple[str, ...] | None = None,
    # Custom plugin caches
    plugin_cache_root: str | None = None,
    plugins: list[str] | tuple[str, ...] | None = None,
    # Output mode
    output_vcf: str | None = None,
    preserve_record_layout: bool = True,
    show_progress: bool = True,
    compression: str | None = None,
    on_batch_written: Callable[[int, int, int], None] | None = None,
) -> pl.LazyFrame | str:
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
        e.g. ``"/data/vep/wgs/parquet/115_GRCh38_ensembl"``.
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
    gencode_basic : bool
        Restrict to transcripts in the GENCODE basic set. Mutually exclusive
        with ``gencode_primary``.
    gencode_primary : bool
        Restrict to transcripts in the GENCODE primary set (GRCh38 only).
        Mutually exclusive with ``gencode_basic``.
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
    buffer_size : int
        Number of input variants per VEP-style annotation buffer. Defaults to
        Ensembl VEP's ``--buffer_size`` default of ``5000``.
    failed : int
        Maximum allowed ``failed`` flag value from cache (default: 0).
    cache_format : str
        Cache format to use. Only ``"parquet"`` is supported (default).
    expected_cache_version : str or None
        Optional assertion against the cache version embedded in each requested
        chromosome's Parquet metadata. It cannot supply missing cache identity.
    cache_size_mb : int
        Annotation cache size in MB (default: 1024).
    workers : int
        Number of within-contig fused annotation pipelines (default: 1) when
        writing with ``output_vcf``; values greater than 1 require a
        tabix-indexed (bgzip + ``.tbi``) input VCF. The ``LazyFrame`` path is
        serial (``workers=1``) in this release.
    skip_csq : bool
        Exclude the raw CSQ column from the output (default: True).
        When True, only the parsed annotation columns are returned.
    fields : {"core"}, list or tuple of str, or None
        Ordered base CSQ fields to emit, mirroring VEP ``--fields``. ``"core"``
        selects VEP's eleven VCF-side default output fields. A list or tuple
        preserves its supplied order. Plugin fields are always appended after
        the selected base block. On the DataFrame path, unselected annotation
        columns are omitted. ``None`` (default) keeps the full layout.
    plugin_cache_root : str or None
        Root of a plugin cache tree built by :func:`build_plugin_cache`, e.g.
        ``"/data/plugin_cache"`` holding ``plugin/<name>/`` directories. Every
        plugin found under the root is applied — see ``plugins`` to narrow
        that — and its CSQ fields are added to the header and to each
        transcript. A plugin's version is fixed when its cache is built, not
        chosen here. ``None`` (default) applies no plugins and is
        byte-identical to a plugin-free run.

        Plugin values are appended to the ``CSQ`` field. With ``fields`` set,
        a ``LazyFrame`` also exposes each plugin field as a named list column,
        including when ``skip_csq=True``.
    plugins : list or tuple of str, or None
        Restrict annotation to these plugin names, a subset of the directories
        under ``<plugin_cache_root>/plugin/``. ``None`` (default) applies every
        plugin found there in alphabetical order. A supplied sequence is also
        the emitted CSQ block order; an empty sequence applies none. Requires
        ``plugin_cache_root``. Duplicate or unknown names are errors.
    output_vcf : str or None
        Path to write annotated VCF output. When set, annotation results are
        written directly to a VCF file and the output path is returned.
        When ``None`` (default), returns a polars ``LazyFrame``.
        Compression is auto-detected from the file extension: ``.vcf`` for
        plain text, ``.vcf.gz`` or ``.vcf.bgz`` for block-gzipped (bgzf).
        Override with the ``compression`` parameter.
    preserve_record_layout : bool
        Write each record's INFO fields in the order the input wrote them, and
        its own FORMAT keys (default: True). Both are per record and neither
        survives the typed columns, so turning this off reorders INFO to schema
        order and drops any FORMAT key whose value is missing in every sample.
        Ensembl VEP keeps both by copying the input line and only appending to
        INFO, so byte agreement with it needs this on. Only used when
        ``output_vcf`` is set.
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

    Notes
    -----
    Filtering the returned ``LazyFrame`` on ``chrom``, ``start`` or ``end``
    restricts the *input* before annotation (region pushdown): unselected
    contigs are skipped and indexed inputs are read by seek. Results are
    identical to filtering after ``collect()``. A ``RuntimeWarning`` is
    raised when the input has no ``.tbi``/``.csi`` index. See the "Region
    filters" section of the Polars DataFrames docs page for the recognised
    predicate shapes.

    Returns
    -------
    polars.LazyFrame or str
        When ``output_vcf`` is ``None``: annotated variants as a polars
        ``LazyFrame`` with typed annotation columns plus original VCF fields.
        A ``select()`` on it decides which annotation flags the engine runs:
        the groups no selected column needs are switched off, and, when no
        flag was given, the groups a column needs are switched on (HGVS and
        the ``everything`` extras require ``reference_fasta``). Collected
        without a ``select()`` and without flags, the frame is the
        ``everything`` result when ``reference_fasta`` is given and the
        co-located lookup result otherwise. ``fields`` cannot be combined
        with a narrowing ``select()``.
        When ``output_vcf`` is set: the output VCF file path.

    Examples
    --------
    >>> import vepyr
    >>> lf = vepyr.annotate("input.vcf", "/data/vep/parquet/115_GRCh38_ensembl")
    >>> lf.collect()

    >>> # Full annotation with all features
    >>> lf = vepyr.annotate(
    ...     "input.vcf",
    ...     "/data/vep/parquet/115_GRCh38_ensembl",
    ...     everything=True,
    ...     reference_fasta="/ref/GRCh38.fa",
    ... )

    >>> # Selective: HGVS + allele frequencies
    >>> lf = vepyr.annotate(
    ...     "input.vcf",
    ...     "/data/vep/parquet/115_GRCh38_ensembl",
    ...     hgvs=True,
    ...     af=True,
    ...     af_gnomadg=True,
    ...     reference_fasta="/ref/GRCh38.fa",
    ... )

    >>> # Write annotated VCF directly
    >>> path = vepyr.annotate(
    ...     "input.vcf",
    ...     "/data/vep/parquet/115_GRCh38_ensembl",
    ...     everything=True,
    ...     reference_fasta="/ref/GRCh38.fa",
    ...     output_vcf="annotated.vcf",
    ... )
    """
    import json

    if fields == "core":
        selected_fields = list(_CORE_CSQ_FIELDS)
    elif isinstance(fields, str):
        raise ValueError("fields must be 'core', an ordered list or tuple, or None")
    elif fields is None:
        selected_fields = None
    elif not isinstance(fields, (list, tuple)):
        raise TypeError("fields must be 'core', an ordered list or tuple, or None")
    else:
        if not fields:
            raise ValueError("fields must contain at least one base CSQ field")
        for name in fields:
            if not isinstance(name, str):
                raise TypeError(
                    f"field names must be strings, got {type(name).__name__}"
                )
        if len(set(fields)) != len(fields):
            raise ValueError("fields must not contain duplicate names")
        selected_fields = list(fields)

    # Validate reference_fasta requirement
    if (everything or hgvs or hgvsc or hgvsp) and not reference_fasta:
        raise ValueError(
            "reference_fasta is required when everything/hgvs/hgvsc/hgvsp=True"
        )

    if gencode_basic and gencode_primary:
        raise ValueError("gencode_basic and gencode_primary are mutually exclusive")
    if (
        isinstance(buffer_size, bool)
        or not isinstance(buffer_size, int)
        or buffer_size <= 0
    ):
        raise ValueError("buffer_size must be a positive integer")
    if isinstance(workers, bool) or not isinstance(workers, int) or workers <= 0:
        raise ValueError("workers must be a positive integer")
    _require_index_for_workers(vcf, workers)
    if cache_format != "parquet":
        raise ValueError("cache_format must be 'parquet'")
    _validate_expected_cache_version(expected_cache_version)

    # Build options JSON — all flags pass through to the engine.
    opts: dict = {
        "extended_probes": extended_probes,
        "cache_format": cache_format,
        "buffer_size": buffer_size,
    }
    if expected_cache_version is not None:
        opts["expected_cache_version"] = expected_cache_version

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
    if workers > 1:
        # Single annotation-concurrency knob: N within-contig fused pipelines.
        # Requires a tabix-indexed (bgzip+.tbi) input VCF.
        opts["workers"] = workers
    if selected_fields is not None:
        opts["fields"] = selected_fields
    if plugins is not None:
        if plugin_cache_root is None:
            raise ValueError("plugins requires plugin_cache_root")
        if isinstance(plugins, str) or not isinstance(plugins, (list, tuple)):
            raise TypeError("plugins must be a list or tuple of plugin names, or None")
        for name in plugins:
            if not isinstance(name, str):
                raise TypeError(
                    f"plugin names must be strings, got {type(name).__name__}"
                )
        if len(set(plugins)) != len(plugins):
            raise ValueError("plugins must not contain duplicate names")
        if plugins:
            source_root = os.path.join(plugin_cache_root, "plugin")
            if not os.path.isdir(source_root):
                raise FileNotFoundError(
                    f"No plugin directory under plugin_cache_root: {source_root}"
                )
            available = sorted(
                name
                for name in os.listdir(source_root)
                if os.path.isfile(os.path.join(source_root, name, "manifest.json"))
            )
            unknown = [name for name in plugins if name not in available]
            if unknown:
                raise ValueError(
                    f"Unknown plugin {unknown[0]!r} in {source_root}. "
                    f"Available: {', '.join(available) or '(none)'}"
                )
            opts["plugin_cache_root"] = plugin_cache_root
            opts["plugins"] = list(plugins)
    elif plugin_cache_root is not None:
        opts["plugin_cache_root"] = plugin_cache_root
    if not preserve_record_layout:
        opts["preserve_record_layout"] = False

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

    # Plugin CSQ fields are named list columns whenever a plugin cache is
    # configured. The engine appends them after the base layout, whose length
    # depends on the flags, so they are read as the last fields of each entry.
    plugin_field_names: list[str] = []
    # (csq_field, element dtype, per_variant): a plugin keyed only on the
    # variant (no match_columns) carries one value per row, a per-feature
    # plugin one value per consequence entry.
    plugin_column_specs: list[tuple[str, pl.DataType, bool]] = []
    # Fields a plugin's match templates read (``{HGVSc}`` say), per plugin
    # column: reading the column needs those fields' flags too.
    plugin_column_inputs: dict[str, set[str]] = {}
    if plugin_cache_root is not None and plugins != []:
        plugin_root = os.path.join(plugin_cache_root, "plugin")
        if not os.path.isdir(plugin_root):
            raise FileNotFoundError(
                f"No plugin directory under plugin_cache_root: {plugin_root}"
            )
        manifests = []
        for directory_name in sorted(os.listdir(plugin_root)):
            manifest_path = os.path.join(plugin_root, directory_name, "manifest.json")
            if not os.path.isfile(manifest_path):
                continue
            with open(manifest_path, encoding="utf-8") as handle:
                manifest = json.load(handle)
            plugin_name = manifest.get("plugin_name", directory_name)
            manifests.append((plugin_name, manifest))
        manifests.sort(key=lambda item: item[0])
        by_name = {name: manifest for name, manifest in manifests}
        ordered_names = list(plugins) if plugins is not None else sorted(by_name)
        for plugin_name in ordered_names:
            manifest = by_name[plugin_name]
            value_columns = manifest.get("value_columns", [])
            if manifest.get("field_order", "declared") == "alphabetical":
                value_columns = sorted(
                    value_columns, key=lambda column: column["csq_field"]
                )
            match_columns = manifest.get("match_columns") or []
            per_variant = not match_columns
            template_fields = {
                field
                for match in match_columns
                for field in re.findall(r"\{([^{}]+)\}", match.get("template", ""))
            }
            for column in value_columns:
                dtype = _plugin_value_dtype(column.get("type"))
                plugin_field_names.append(column["csq_field"])
                plugin_column_specs.append((column["csq_field"], dtype, per_variant))
                plugin_column_inputs[column["csq_field"]] = template_fields
        if len(set(plugin_field_names)) != len(plugin_field_names):
            raise ValueError(
                "selected plugins expose duplicate CSQ field names, which cannot be "
                "represented as distinct DataFrame columns"
            )

    # Get schema from a probe annotator (doesn't consume data).
    engine_skip_csq = skip_csq and not plugin_field_names
    probe = _create_annotator(
        vcf,
        cache_dir,
        options_json,
        engine_skip_csq,
        None,
    )
    pa_schema = probe.schema
    empty = pa.table({field.name: pa.array([], type=field.type) for field in pa_schema})
    polars_schema = dict(pl.from_arrow(empty).schema)
    selected_dataframe_columns: list[str] | None = None
    if selected_fields is not None:
        schema_names = list(polars_schema)
        try:
            annotation_start = schema_names.index("most_severe_consequence")
        except ValueError as exc:
            raise RuntimeError(
                "annotation schema is missing most_severe_consequence"
            ) from exc
        annotation_schema_names = set(schema_names[annotation_start + 1 :])
        missing_columns = [
            name for name in selected_fields if name not in annotation_schema_names
        ]
        if missing_columns:
            raise ValueError(
                "selected CSQ fields have no named DataFrame column: "
                + ", ".join(missing_columns)
            )
        selected_dataframe_columns = [
            name
            for name in schema_names[: annotation_start + 1]
            if not (skip_csq and name == "CSQ")
        ]
        selected_dataframe_columns.extend(selected_fields)
        polars_schema = {
            name: polars_schema[name] for name in selected_dataframe_columns
        }
    if plugin_field_names:
        for name, dtype, per_variant in plugin_column_specs:
            if name in polars_schema:
                raise ValueError(
                    f"plugin CSQ field {name!r} conflicts with an existing DataFrame column"
                )
            polars_schema[name] = dtype if per_variant else pl.List(dtype)
        if skip_csq:
            polars_schema.pop("CSQ", None)
    del probe

    # Each collect() creates a fresh streaming annotator so the LazyFrame
    # is re-runnable (not single-use). Captures vcf/cache_dir/options by value.
    _vcf, _cache_dir, _opts, _engine_skip = (
        vcf,
        cache_dir,
        dict(opts),
        engine_skip_csq,
    )
    plugin_columns = set(plugin_field_names)

    # Region pushdown state, per annotate() call: header contigs are read
    # lazily on the first collect that carries a genomic predicate, and the
    # missing-index warning is raised at most once.
    _region_state: dict = {"contigs": None, "warned": False}

    def _header_contigs() -> list[str]:
        if _region_state["contigs"] is None:
            _region_state["contigs"] = list(_vcf_contigs(_vcf))
        return _region_state["contigs"]

    def _warn_if_unindexed() -> None:
        if _region_state["warned"]:
            return
        _region_state["warned"] = True
        if os.path.exists(_vcf + ".tbi") or os.path.exists(_vcf + ".csi"):
            return
        warnings.warn(
            f"region filter on {_vcf!r} without a tabix/CSI index ({_vcf}.tbi or "
            ".csi): the whole file is parsed once to find its contigs and once "
            "more to filter it before annotation. Compress with bgzip and index "
            "with tabix for seek-based reads.",
            RuntimeWarning,
            stacklevel=2,
        )

    def _batch_source(with_columns, predicate, n_rows, batch_size):
        # Projection pushdown: the columns Polars asks for, plus the ones the
        # pushed-down filter reads, decide which annotation flags the engine
        # needs. n_rows becomes a LIMIT in the DataFusion query.
        needed = None
        if with_columns is not None and set(with_columns) != set(polars_schema):
            if "fields" in _opts:
                raise ValueError(
                    "annotate(fields=...) already fixes the annotation layout; "
                    "select columns on the LazyFrame or pass fields=, not both"
                )
            needed = set(with_columns)
            if predicate is not None:
                needed.update(predicate.meta.root_names())
        # Fields the match templates of every plugin column read, this query
        # included, must be computed whatever flags were given: a plugin whose
        # template needs HGVSc is null without hgvs. The raw CSQ string carries
        # every plugin's values, so reading it reads every plugin.
        if needed is None or "CSQ" in needed:
            read_plugins = plugin_columns
        else:
            read_plugins = needed & plugin_columns
        required = set().union(*(plugin_column_inputs[c] for c in read_plugins))
        engine_opts = _flags_for_projection(_opts, needed, set(polars_schema), required)
        # Predicate pushdown on genomic coordinates: chrom/start/end conjuncts
        # become engine `regions`, so unselected contigs are never prepared and
        # indexed inputs are read by seek. Polars still applies the full
        # predicate on every batch below, so this can only narrow the input.
        if predicate is not None and GENOMIC_COLUMNS & set(predicate.meta.root_names()):
            # The contig list is fetched lazily: only a pushable predicate
            # pays for the contig scan an unindexed input needs.
            regions = extract_regions(predicate, _header_contigs)
            if regions == []:
                return
            if regions is not None:
                _warn_if_unindexed()
                engine_opts["regions"] = regions
        # The CSQ string is only built when the query reads it or a plugin
        # column parsed out of it.
        if needed is None:
            csq_needed = not _engine_skip
        else:
            csq_needed = "CSQ" in needed or bool(needed & plugin_columns)
            if not csq_needed:
                # Plugin values only ever reach the frame through the CSQ
                # string, so a query reading neither skips the plugin lookup.
                engine_opts.pop("plugin_cache_root", None)
                engine_opts.pop("plugins", None)
        annotator = _create_annotator(
            _vcf,
            _cache_dir,
            json.dumps(engine_opts),
            not csq_needed,
            n_rows,
        )
        remaining = n_rows
        for py_batch in annotator:
            batch_df = pl.from_arrow(py_batch)
            if plugin_field_names and "CSQ" in batch_df.columns:
                n_plugin = len(plugin_field_names)
                batch_df = batch_df.with_columns(
                    _plugin_column(
                        pl.col("CSQ")
                        .str.split(",")
                        .list.eval(
                            pl.element()
                            .str.split("|")
                            .list.get(index - n_plugin, null_on_oob=True)
                            .replace("", None)
                        ),
                        dtype,
                        per_variant,
                    ).alias(name)
                    for index, (name, dtype, per_variant) in enumerate(
                        plugin_column_specs
                    )
                )
            if skip_csq and "CSQ" in batch_df.columns:
                batch_df = batch_df.drop("CSQ")
            if selected_dataframe_columns is not None:
                batch_df = batch_df.select(
                    [
                        *selected_dataframe_columns,
                        *(c for c in plugin_field_names if c in batch_df.columns),
                    ]
                )
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
