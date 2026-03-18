from __future__ import annotations

import logging

from vepyr._core import cache_to_parquet as _cache_to_parquet

__all__ = ["build_cache"]

log = logging.getLogger(__name__)

# Ensembl FTP URL template for VEP cache tarballs
_ENSEMBL_FTP = (
    "https://ftp.ensembl.org/pub/release-{release}"
    "/variation/vep/{species}_vep_{release}_{assembly}.tar.gz"
)


def _download_with_progress(url: str, dest: str) -> None:
    """Download a file with a tqdm progress bar."""
    import urllib.request

    from tqdm import tqdm

    req = urllib.request.Request(url)
    with urllib.request.urlopen(req) as response:
        total = int(response.headers.get("Content-Length", 0))
        with (
            tqdm(
                total=total,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=dest.rsplit("/", 1)[-1],
            ) as pbar,
            open(dest, "wb") as f,
        ):
            while True:
                chunk = response.read(1024 * 1024)  # 1 MB chunks
                if not chunk:
                    break
                f.write(chunk)
                pbar.update(len(chunk))


def build_cache(
    release: int,
    cache_dir: str,
    *,
    species: str = "homo_sapiens",
    assembly: str = "GRCh38",
    partitions: int = 8,
) -> list[tuple[str, int]]:
    """Download an Ensembl VEP cache and convert it to optimized Parquet files.

    Parameters
    ----------
    release : int
        Ensembl release number (e.g. 115).
    cache_dir : str
        Directory where the cache will be downloaded, unpacked, and Parquet
        files will be written.
    species : str
        Species name (default: ``"homo_sapiens"``).
    assembly : str
        Genome assembly (default: ``"GRCh38"``).
    partitions : int
        Number of DataFusion partitions for parallelism (default: 8).

    Returns
    -------
    list[tuple[str, int]]
        List of ``(parquet_file_path, row_count)`` for each written file.
    """
    import os
    import tarfile

    # Paths
    tarball_name = f"{species}_vep_{release}_{assembly}.tar.gz"
    tarball_path = os.path.join(cache_dir, tarball_name)
    # The tarball unpacks to <species>/<release>_<assembly>/
    cache_root = os.path.join(cache_dir, species, f"{release}_{assembly}")
    output_dir = os.path.join(cache_dir, "parquet")

    os.makedirs(cache_dir, exist_ok=True)

    # Download if not already present
    if not os.path.isdir(cache_root):
        url = _ENSEMBL_FTP.format(
            release=release, species=species, assembly=assembly
        )
        if not os.path.isfile(tarball_path):
            _download_with_progress(url, tarball_path)

        # Extract
        log.info("Extracting %s ...", tarball_path)
        with tarfile.open(tarball_path) as tar:
            tar.extractall(path=cache_dir)
        log.info("Extracted to %s", cache_root)

        # Clean up tarball
        os.remove(tarball_path)

    if not os.path.isdir(cache_root):
        raise FileNotFoundError(
            f"Cache directory not found after extraction: {cache_root}"
        )

    # Convert to Parquet
    log.info("Converting cache to Parquet (%d partitions) ...", partitions)
    results = _cache_to_parquet(cache_root, output_dir, partitions)
    log.info("Done. Wrote %d Parquet files to %s", len(results), output_dir)
    return results
