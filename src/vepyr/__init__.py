from __future__ import annotations

import logging

from vepyr._core import convert_entity as _convert_entity

__all__ = ["build_cache"]

log = logging.getLogger(__name__)

# Ensembl FTP URL templates for VEP cache tarballs.
# Release >=115 uses indexed_vep_cache/, older releases use vep/.
_ENSEMBL_FTP_PATHS = [
    "https://ftp.ensembl.org/pub/release-{release}/variation/indexed_vep_cache/{species}_vep_{release}_{assembly}.tar.gz",
    "https://ftp.ensembl.org/pub/release-{release}/variation/vep/{species}_vep_{release}_{assembly}.tar.gz",
]


def _download_with_progress(url: str, dest: str) -> None:
    """Download a file with a tqdm progress bar.

    Tracks progress by bytes written to disk to avoid urllib transparent
    decompression inflating the byte count beyond Content-Length.
    """
    import http.client
    import os
    import urllib.parse

    from tqdm import tqdm

    filename = dest.rsplit("/", 1)[-1]
    log.info("Downloading %s", url)

    # Use http.client directly to avoid urllib's transparent decompression
    # which inflates byte counts for .tar.gz files
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
    release: int, species: str, assembly: str, dest: str
) -> None:
    """Try FTP URL patterns and download the cache tarball."""
    import urllib.error

    for pattern in _ENSEMBL_FTP_PATHS:
        url = pattern.format(release=release, species=species, assembly=assembly)
        try:
            _download_with_progress(url, dest)
            return
        except urllib.error.HTTPError as e:
            if e.code == 404:
                log.debug("Not found: %s", url)
                continue
            raise
    raise FileNotFoundError(
        f"VEP cache not found for {species} release {release} assembly {assembly}. "
        f"Browse available caches at "
        f"https://ftp.ensembl.org/pub/release-{release}/variation/"
    )


def build_cache(
    release: int,
    cache_dir: str,
    *,
    species: str = "homo_sapiens",
    assembly: str = "GRCh38",
    partitions: int = 8,
    local_cache: str | None = None,
) -> list[tuple[str, int]]:
    """Download an Ensembl VEP cache and convert it to optimized Parquet files.

    Parameters
    ----------
    release : int
        Ensembl release number (e.g. 115).
    cache_dir : str
        Directory where Parquet files will be written.
    species : str
        Species name (default: ``"homo_sapiens"``).
    assembly : str
        Genome assembly (default: ``"GRCh38"``).
    partitions : int
        Number of DataFusion partitions for parallelism (default: 8).
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

    if local_cache is not None:
        cache_root = local_cache
        if not os.path.isdir(cache_root):
            raise FileNotFoundError(
                f"Local cache directory not found: {cache_root}"
            )
        log.info("Using local cache: %s", cache_root)
    else:
        # Paths
        tarball_name = f"{species}_vep_{release}_{assembly}.tar.gz"
        tarball_path = os.path.join(cache_dir, tarball_name)
        # The tarball unpacks to <species>/<release>_<assembly>/
        cache_root = os.path.join(cache_dir, species, f"{release}_{assembly}")

        os.makedirs(cache_dir, exist_ok=True)

        # Download if not already present
        if not os.path.isdir(cache_root):
            if not os.path.isfile(tarball_path):
                _download_cache(release, species, assembly, tarball_path)

            # Extract
            tarball_size_mb = os.path.getsize(tarball_path) / (1024 * 1024)
            log.info(
                "Extracting %s (%.0f MB) ...", tarball_name, tarball_size_mb
            )
            with tarfile.open(tarball_path) as tar:
                tar.extractall(path=cache_dir)
            log.info("Extracted to %s", cache_root)

            # Clean up tarball
            os.remove(tarball_path)

        if not os.path.isdir(cache_root):
            raise FileNotFoundError(
                f"Cache directory not found after extraction: {cache_root}"
            )

    output_dir = os.path.join(cache_dir, "parquet")

    # Convert each entity type to Parquet with progress
    from tqdm.auto import tqdm

    entities = [
        "variation", "transcript", "exon",
        "translation", "regulatory", "motif",
    ]
    all_results: list[tuple[str, int]] = []
    for entity in tqdm(entities, desc="Converting to Parquet", unit="entity"):
        result = _convert_entity(cache_root, output_dir, entity, partitions)
        if result is None:
            log.info("Skipping %s: no source files found", entity)
            continue
        for path, rows in result:
            log.info(
                "%s: %s rows -> %s",
                entity, f"{rows:,}", os.path.basename(path),
            )
        all_results.extend(result)

    log.info("Done. Wrote %d Parquet files to %s", len(all_results), output_dir)
    return all_results
