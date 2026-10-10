"""The Ensembl release-116 VEP cache and GRCh38 FASTA: check, fetch, record.

Both artefacts come from Ensembl's public FTP (served over HTTPS). Each is
pinned by the ``sum`` value Ensembl publishes in the ``CHECKSUMS`` file next to
it (BSD checksum + 1 KiB block count, the format Ensembl uses); ``bless`` also
computes the archive's sha256 while downloading and records both.

A download is a parallel, resumable set of HTTP range requests: Ensembl's FTP
throttles single connections to roughly 1 MB/s, which would make the 27.6 GB
cache an all-day fetch. Finished chunks survive an interrupted run, so running
the same command again continues where it stopped.

After a successful fetch a small provenance file is written next to the
artefact (``.bless-source.toml`` in the cache root, ``<fasta>.bless-source.toml``
beside the FASTA). A later run that points ``--vep-cache-dir``/``--vep-fasta``
at the same path reads it, so the source URL and checksum are recorded however
the path was supplied.
"""

from __future__ import annotations

import gzip
import hashlib
import shutil
import subprocess
import sys
import tarfile
import time
import tomllib
import urllib.error
import urllib.request
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import fixture_sources

from bless import BlessError
from bless.testdir import write_atomically

SPECIES: Final[str] = "homo_sapiens"
ASSEMBLY: Final[str] = "GRCh38"
RELEASE: Final[int] = 116
CACHE_SUBDIR: Final[str] = f"{SPECIES}/{RELEASE}_{ASSEMBLY}"
"""Where a release-116 GRCh38 human cache lives inside ``--dir_cache``."""

REQUIRED_CONTIGS: Final[tuple[str, ...]] = (
    *(str(n) for n in range(1, 23)),
    "X",
    "Y",
    "MT",
)
"""Chromosome directories a complete primary-assembly cache must contain."""

SOURCE_RECORD: Final[str] = ".bless-source.toml"
_BASE: Final[str] = f"https://ftp.ensembl.org/pub/release-{RELEASE}"


@dataclass(frozen=True, slots=True, kw_only=True)
class Artefact:
    """One pinned download.

    Attributes:
        url: HTTPS URL on Ensembl's public FTP.
        ensembl_sum: ``sum`` output as listed in Ensembl's ``CHECKSUMS`` file.
        size: Size in bytes (from the server, pinned so a truncated or
            re-published file is caught before the checksum runs).
    """

    url: str
    ensembl_sum: str
    size: int

    @property
    def filename(self) -> str:
        """Last path component of :attr:`url`."""
        return self.url.rsplit("/", 1)[-1]


CACHE: Final[Artefact] = Artefact(
    url=f"{_BASE}/variation/indexed_vep_cache/{SPECIES}_vep_{RELEASE}_{ASSEMBLY}.tar.gz",
    ensembl_sum="56036 26996736",
    size=27_644_657_162,
)
FASTA: Final[Artefact] = Artefact(
    url=f"{_BASE}/fasta/{SPECIES}/dna/Homo_sapiens.{ASSEMBLY}.dna.primary_assembly.fa.gz",
    ensembl_sum="22450 861294",
    size=881_964_081,
)

_CHUNK: Final[int] = 128 * 1024 * 1024
_WORKERS: Final[int] = 24
_RETRIES: Final[int] = 6
_IO_BLOCK: Final[int] = 4 * 1024 * 1024


@dataclass(frozen=True, slots=True, kw_only=True)
class Provenance:
    """Where a cache or FASTA came from, as recorded in ``test.toml``.

    Attributes:
        source: Recorded download URL, or the declared URL when unverified.
        checksum: ``sha256:<hex> sum:<ensembl sum>`` of the fetched archive, or
            ``unverified`` when the path was not fetched by ``bless``.
    """

    source: str
    checksum: str


def missing_cache_parts(cache_dir: Path, *, merged: bool = False) -> list[str]:
    """List what a directory lacks to be a complete release-116 GRCh38 cache.

    Args:
        cache_dir: Candidate ``--dir_cache`` root.

    Returns:
        Missing relative paths; empty when complete.
    """
    subdir = (
        CACHE_SUBDIR.replace(SPECIES, SPECIES + "_merged") if merged else CACHE_SUBDIR
    )
    base = cache_dir / subdir
    if not base.is_dir():
        return [subdir + "/"]
    missing = [] if (base / "info.txt").is_file() else [f"{subdir}/info.txt"]
    missing += [f"{subdir}/{c}/" for c in REQUIRED_CONTIGS if not (base / c).is_dir()]
    return missing


def require_complete_cache(cache_dir: Path, *, flag: str, merged: bool = False) -> None:
    """Refuse a cache directory that is not complete; never writes into it.

    Args:
        cache_dir: Directory to check.
        flag: The flag it came from, for the message.

    Raises:
        BlessError: Naming the first missing parts.
    """
    missing = missing_cache_parts(cache_dir, merged=merged)
    if missing:
        shown = ", ".join(missing[:5]) + (
            f" (+{len(missing) - 5} more)" if len(missing) > 5 else ""
        )
        raise BlessError(
            f"{flag} {cache_dir} is not a complete release-{RELEASE} {ASSEMBLY} "
            f"{SPECIES} VEP cache (missing: {shown}); point {flag} at a complete "
            "cache or use --download-vep-cache-to-dir to fetch one"
        )


def require_fasta(fasta: Path, *, flag: str) -> None:
    """Refuse a FASTA path that is missing, unreadable or not plain FASTA.

    Args:
        fasta: File to check.
        flag: The flag it came from, for the message.

    Raises:
        BlessError: With the reason.
    """
    try:
        with fasta.open("rb") as fh:
            head = fh.read(1)
    except OSError as exc:
        raise BlessError(
            f"{flag} {fasta} cannot be read ({exc.strerror or exc}); pass an existing "
            "uncompressed FASTA or use --download-vep-fasta-to"
        ) from exc
    if head != b">":
        raise BlessError(
            f"{flag} {fasta} is not an uncompressed FASTA (first byte is not '>'); "
            "pass the decompressed .fa or use --download-vep-fasta-to"
        )


def read_provenance(record: Path) -> Provenance | None:
    """Read a provenance file written by a previous download.

    Args:
        record: Path of the provenance file.

    Returns:
        The provenance, or ``None`` when there is no (readable) record.
    """
    try:
        data = tomllib.loads(record.read_text(encoding="utf-8"))
        return Provenance(source=str(data["source"]), checksum=str(data["checksum"]))
    except (OSError, tomllib.TOMLDecodeError, KeyError):
        return None


def cache_provenance(cache_dir: Path, *, merged: bool = False) -> Provenance:
    """Provenance of a cache directory, fetched by ``bless`` or not."""
    if merged:
        # The root's download receipt describes the Ensembl archive, not merged.
        return Provenance(
            source=fixture_sources.vep_cache("merged"),
            checksum="unverified",
        )
    prov = read_provenance(cache_dir / SOURCE_RECORD)
    if prov is not None and fixture_sources.is_download_url(prov.source):
        return prov
    return Provenance(source=CACHE.url, checksum="unverified")


def fasta_provenance(fasta: Path) -> Provenance:
    """Provenance of a FASTA file, fetched by ``bless`` or not."""
    prov = read_provenance(fasta.with_name(fasta.name + SOURCE_RECORD))
    if prov is not None and fixture_sources.is_download_url(prov.source):
        return prov
    return Provenance(source=FASTA.url, checksum="unverified")


def _write_provenance(record: Path, prov: Provenance) -> None:
    """Persist ``prov`` at ``record``."""
    write_atomically(
        record, f'source = "{prov.source}"\nchecksum = "{prov.checksum}"\n'
    )


def _log(message: str) -> None:
    """Progress line on stderr."""
    print(f"bless: {message}", file=sys.stderr, flush=True)


def _fetch_chunk(url: str, first: int, last: int, dest: Path) -> None:
    """Download bytes ``first..last`` (inclusive) of ``url`` into ``dest``.

    Retries with backoff; a chunk is renamed into place only when complete.

    Raises:
        BlessError: After the last retry fails.
    """
    want = last - first + 1
    tmp = dest.with_suffix(".tmp")
    error: Exception | None = None
    for attempt in range(_RETRIES):
        try:
            req = urllib.request.Request(
                url, headers={"Range": f"bytes={first}-{last}"}
            )
            with (
                urllib.request.urlopen(req, timeout=120) as resp,
                tmp.open("wb") as out,
            ):
                if resp.status != 206:
                    raise BlessError(f"{url} does not honour HTTP range requests")
                shutil.copyfileobj(resp, out, _IO_BLOCK)
            if tmp.stat().st_size != want:
                raise OSError(f"short read: {tmp.stat().st_size} of {want} bytes")
            tmp.replace(dest)
            return
        except BlessError:
            raise
        except (OSError, urllib.error.URLError) as exc:
            error = exc
            time.sleep(min(60, 5 * 2**attempt))
    raise BlessError(f"download of {url} failed after {_RETRIES} attempts: {error}")


def _download(art: Artefact, staging: Path) -> tuple[Path, str]:
    """Fetch ``art`` into ``staging``, verify it, return (archive, sha256 hex).

    Raises:
        BlessError: On network failure or checksum mismatch.
    """
    staging.mkdir(parents=True, exist_ok=True)
    archive = staging / art.filename
    if (
        archive.is_file()
        and archive.stat().st_size == art.size
        and _ensembl_sum(archive) == art.ensembl_sum
    ):
        _log(f"{archive} already downloaded and verified; hashing it")
        sha = hashlib.sha256()
        with archive.open("rb") as fh:
            while block := fh.read(_IO_BLOCK):
                sha.update(block)
        return archive, sha.hexdigest()
    ranges = [
        (i, s, min(s + _CHUNK, art.size) - 1)
        for i, s in enumerate(range(0, art.size, _CHUNK))
    ]
    todo = [r for r in ranges if not (staging / f"{r[0]:05d}.part").is_file()]
    _log(
        f"fetching {art.url} ({art.size / 1e9:.1f} GB, {len(ranges)} chunks, "
        f"{len(ranges) - len(todo)} already present) into {staging}"
    )
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=_WORKERS) as pool:
        futures = [
            pool.submit(_fetch_chunk, art.url, a, b, staging / f"{i:05d}.part")
            for i, a, b in todo
        ]
        for done, fut in enumerate(as_completed(futures), 1):
            fut.result()
            if done % 4 == 0 or done == len(futures):
                _log(
                    f"{art.filename}: {done}/{len(futures)} chunks "
                    f"({time.monotonic() - started:.0f}s)"
                )
    sha = hashlib.sha256()

    def chunks() -> Iterator[bytes]:
        for i, _, _ in ranges:
            part = staging / f"{i:05d}.part"
            with part.open("rb") as fh:
                while block := fh.read(_IO_BLOCK):
                    sha.update(block)
                    yield block
            part.unlink()

    write_atomically(archive, chunks())
    if (size := archive.stat().st_size) != art.size:
        raise BlessError(f"{archive}: size {size} != pinned {art.size}")
    got = _ensembl_sum(archive)
    if got != art.ensembl_sum:
        archive.unlink(missing_ok=True)
        raise BlessError(
            f"checksum mismatch for {art.url}: `sum` gives {got!r}, Ensembl CHECKSUMS "
            f"lists {art.ensembl_sum!r}; the partial download was removed, "
            "re-run to retry"
        )
    _log(
        f"{art.filename}: verified (sum {got}, sha256 {sha.hexdigest()}) "
        f"in {time.monotonic() - started:.0f}s"
    )
    return archive, sha.hexdigest()


def _ensembl_sum(path: Path) -> str:
    """Return ``sum``'s ``checksum blocks`` for ``path`` (Ensembl CHECKSUMS format).

    Raises:
        BlessError: If ``sum`` is unavailable or fails.
    """
    exe = shutil.which("sum")
    if exe is None:
        raise BlessError(
            "`sum` is not on PATH; it is needed to verify the Ensembl checksum"
        )
    done = subprocess.run([exe, str(path)], capture_output=True, text=True)
    if done.returncode != 0:
        raise BlessError(f"`sum {path}` failed: {done.stderr.strip()}")
    return " ".join(done.stdout.split()[:2])


def download_cache(cache_dir: Path) -> None:
    """Fetch, verify and unpack the release-116 cache into ``cache_dir``.

    A directory that is already complete and carries a provenance record is
    reused as is.

    Raises:
        BlessError: On any failure, including an unpacked tree that is still
            incomplete.
    """
    if not missing_cache_parts(cache_dir) and (cache_dir / SOURCE_RECORD).is_file():
        _log(f"{cache_dir} already holds a verified cache; not downloading again")
        return
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise BlessError(
            f"cannot create --download-vep-cache-to-dir {cache_dir}: {exc.strerror}"
        ) from exc
    staging = cache_dir / ".bless-download"
    archive, sha = _download(CACHE, staging)
    _log(f"unpacking {archive.name} into {cache_dir}")
    try:
        with tarfile.open(archive, "r:gz") as tar:
            tar.extractall(cache_dir, filter="data")
    except (OSError, tarfile.TarError) as exc:
        raise BlessError(f"cannot unpack {archive}: {exc}") from exc
    shutil.rmtree(staging)
    require_complete_cache(cache_dir, flag="--download-vep-cache-to-dir")
    _write_provenance(
        cache_dir / SOURCE_RECORD,
        Provenance(source=CACHE.url, checksum=f"sha256:{sha} sum:{CACHE.ensembl_sum}"),
    )


def download_fasta(fasta: Path) -> None:
    """Fetch, verify and decompress the GRCh38 primary-assembly FASTA to ``fasta``.

    Raises:
        BlessError: On any failure.
    """
    record = fasta.with_name(fasta.name + SOURCE_RECORD)
    if fasta.is_file() and record.is_file():
        _log(f"{fasta} already holds a verified FASTA; not downloading again")
        return
    staging = fasta.with_name(f".{fasta.name}.bless-download")
    try:
        fasta.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise BlessError(
            f"cannot create {fasta.parent} for --download-vep-fasta-to: {exc.strerror}"
        ) from exc
    archive, sha = _download(FASTA, staging)
    _log(f"decompressing {archive.name} to {fasta}")

    def plain() -> Iterator[bytes]:
        with gzip.open(archive, "rb") as fh:
            while block := fh.read(_IO_BLOCK):
                yield block

    write_atomically(fasta, plain())
    shutil.rmtree(staging)
    _write_provenance(
        record,
        Provenance(source=FASTA.url, checksum=f"sha256:{sha} sum:{FASTA.ensembl_sum}"),
    )


def ensure_fai(fasta: Path) -> Path:
    """Return ``<fasta>.fai``, building it (samtools faidx format) when absent.

    The FASTA is mounted into the container read-only, so VEP cannot index it
    itself; building the index here keeps the host directory the only place
    that is written.

    Raises:
        BlessError: If the FASTA is malformed or the index cannot be written.
    """
    fai = fasta.with_name(fasta.name + ".fai")
    if fai.is_file() and fai.stat().st_mtime >= fasta.stat().st_mtime:
        return fai
    _log(f"indexing {fasta}")
    rows: list[str] = []
    name: str | None = None
    length = offset = line_bases = line_bytes = 0
    pos = 0
    with fasta.open("rb") as fh:
        for line in fh:
            if line.startswith(b">"):
                if name is not None:
                    rows.append(
                        f"{name}\t{length}\t{offset}\t{line_bases}\t{line_bytes}\n"
                    )
                name = line[1:].split()[0].decode()
                length = line_bases = line_bytes = 0
                offset = pos + len(line)
            else:
                bases = len(line.rstrip(b"\r\n"))
                if line_bases == 0:
                    line_bases, line_bytes = bases, len(line)
                length += bases
            pos += len(line)
    if name is None:
        raise BlessError(f"{fasta} holds no FASTA record")
    rows.append(f"{name}\t{length}\t{offset}\t{line_bases}\t{line_bytes}\n")
    write_atomically(fai, "".join(rows))
    return fai
