from __future__ import annotations

import logging

from vepyr._core import convert_entity as _convert_entity

__all__ = ["build_cache"]

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
