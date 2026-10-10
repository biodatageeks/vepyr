"""Fetch the pinned VEP cache 116 Parquet files (and the GRCh38 FASTA) into one root.

One cache root (``$VEPYR_CACHE_ROOT``), one revision per flavour, one provenance record.
This is the only code that writes there.

Layout produced (names are the ones the HuggingFace dataset README uses, so a command
copied from an issue and a command from this repository spell the same path)::

    <cache-dir>/
      PROVENANCE.json
      116_GRCh38_ensembl/{exon,motif,regulatory,transcript,translation_core,
                          translation_sift,variation}/{<contig>.parquet,chrom_manifest.json}
      116_GRCh38_refseq/...     116_GRCh38_merged/...
      fasta/Homo_sapiens.GRCh38.dna.primary_assembly.fa (+ .fai)

This module is a library: ``./run_tests`` (:mod:`run_tests.cli`) calls :func:`fetch`
with a :class:`Selection` derived from enabled fixtures. The CLI requests merged
cache shards, trimmed manifests and the full FASTA. Every failure is raised as
:class:`run_tests.verdict.RunTestsError` with a definite :class:`run_tests.verdict.Exit`
code; there is no second CLI here.

The library's dry-run selection writes nothing — not even the directory: it lists
the files the selection names on the Hub and their byte total, and exits 4 when
the selection names no file (an empty plan is never a clean one).

Per-contig roots need a contig every entity carries (chr1-22, chrX, chrY): ``motif`` and
``regulatory`` have no ``chrMT``, so a selection containing only chrMT is refused
(exit 4). Required contigs come from the fixture metadata.

Why the manifests are trimmed in per-contig mode: datafusion-bio-function-vep 0.17.2
opens the FIRST entry of ``variation/chrom_manifest.json`` to detect the cache flavour
(``cache_source.rs:43-58``), and the first shard of every entity when it scans, so a
partial download whose manifests still name ``chr1`` fails on a file that was never
requested. Trimming keeps only entries whose shard is
on disk so annotation can use a partially downloaded cache.

Why ``variation/`` is NOT de-duplicated across flavours: the three shards of one contig
have one size but three sha256 digests (each embeds its own
``bio.vep.cache_source_type`` footer), and the engine reads the flavour off that footer.
A hardlink would silently turn a refseq root into an ensembl one.
"""

from __future__ import annotations

import fcntl
import fnmatch
import gzip
import hashlib
import json
import os
import re
import tomllib
from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Final, Protocol

from run_tests.progress import Progress
from run_tests.verdict import Exit, RunTestsError

__all__ = [
    "CACHE_METADATA",
    "ENTITIES",
    "DatasetPin",
    "FastaPin",
    "Flavour",
    "Provenance",
    "RemoteFile",
    "Selection",
    "allow_patterns",
    "bsd_sum",
    "fetch",
    "fetch_fasta",
    "hub_downloader",
    "hub_lister",
    "load_dataset_pins",
    "plan",
    "provenance_lock",
    "read_provenance",
    "select_remote",
    "trim_manifest",
    "url_fetcher",
    "verify_flavour",
    "write_fai",
    "write_provenance",
]

SCHEMA_VERSION: Final[int] = 1
"""``PROVENANCE.json`` schema; bump when a key changes meaning."""

ENTITIES: Final[tuple[str, ...]] = (
    "exon",
    "motif",
    "regulatory",
    "transcript",
    "translation_core",
    "translation_sift",
    "variation",
)
"""Every entity a complete flavour carries, in the README's order."""

MANIFEST: Final[str] = "chrom_manifest.json"
README: Final[str] = "README.md"
CACHE_METADATA: Final[tuple[str, ...]] = ("chr_synonyms.txt", "reference_policy.json")
"""Root metadata used for chromosome aliases and reference handling."""
PROVENANCE: Final[str] = "PROVENANCE.json"
FASTA_DIR: Final[str] = "fasta"
FASTA_PIN: Final[str] = "grch38_fasta"
PIN_PREFIX: Final[str] = "hf_cache_"
PROVENANCE_LOCK: Final[str] = "PROVENANCE.lock"
"""Lock file beside ``PROVENANCE.json``; held (``fcntl.flock``) across every
read-modify-write so two concurrent runs on one root cannot erase each other."""
_TRANSIENT_NAME: Final[re.Pattern[str]] = re.compile(r"^\..+\.\d+\.tmp$")
"""This tool's own temp names, ``.<name>.<pid>.tmp`` (:func:`_atomic_write_text`) — the
only dot-named files :func:`_on_disk` ignores. Anything else the Hub lists, such as
``.gitattributes``, is a requested file and counts as present when it is.
"""
_DATASET_HOST: Final[str] = "https://huggingface.co/datasets/"
_CHUNK: Final[int] = 1 << 20


class Flavour(StrEnum):
    """The three VEP cache flavours published on the Hub."""

    ENSEMBL = "ensembl"
    REFSEQ = "refseq"
    MERGED = "merged"

    @property
    def dir_name(self) -> str:
        """The directory under the root, as the HF README spells it."""
        return f"116_GRCh38_{self.value}"

    @property
    def pin_name(self) -> str:
        """The ``PINS.toml`` table holding this flavour's revision."""
        return f"{PIN_PREFIX}{self.value}"


@dataclass(frozen=True, slots=True, kw_only=True)
class DatasetPin:
    """One ``role = "dataset"`` pin: where it lives and which revision is canonical."""

    repo_id: str
    revision: str

    @classmethod
    def from_table(cls, name: str, table: dict[str, object]) -> DatasetPin:
        """Build from a ``PINS.toml`` table; only HF dataset URLs become ``repo_id``."""
        repo = str(table["repo"])
        if not repo.startswith(_DATASET_HOST):
            raise RunTestsError(
                Exit.USAGE,
                f"pin {name!r}: repo {repo!r} is not a huggingface.co dataset",
            )
        return cls(
            repo_id=repo.removeprefix(_DATASET_HOST).rstrip("/"),
            revision=str(table["sha"]),
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class FastaPin:
    """The ``[grch38_fasta]`` pin: directory URL, file name, sha256 of the ``.fa``."""

    url: str
    sha256_fa: str
    """sha256 of the UNCOMPRESSED ``.fa`` (``[grch38_fasta].sha``): 64 hex characters,
    validated by :func:`load_dataset_pins`; a git sha or a truncated paste is exit 2,
    never a silently skipped end-to-end check."""
    ensembl_sum: str
    """Ensembl's ``CHECKSUMS`` line for the ``.gz``, e.g. ``22450 861294``.

    Required: a pin without it is refused by :func:`load_dataset_pins`, so a misspelt
    key can never turn the CHECKSUMS comparison into a silent skip.
    """

    @property
    def gz_name(self) -> str:
        """The ``.gz`` name: ``Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz``."""
        return self.url.rsplit("/", 1)[1]

    @property
    def fa_name(self) -> str:
        """The uncompressed file name."""
        return self.gz_name.removesuffix(".gz")


def load_dataset_pins(
    pins_toml: Path,
) -> tuple[dict[Flavour, DatasetPin], FastaPin | None]:
    """Read the ``hf_cache_*`` and ``grch38_fasta`` pins straight out of ``PINS.toml``.

    Read with :mod:`tomllib` rather than through :mod:`pins` so that this tool depends
    on nothing but the stdlib and ``huggingface_hub``; ``tools/test_pins.py`` is where
    the pin contract itself is enforced.

    Args:
        pins_toml: Path to ``PINS.toml``.

    Returns:
        ``(dataset pins by flavour, fasta pin or None)``.

    Raises:
        RunTestsError: :attr:`Exit.USAGE` when the file is absent, malformed, lacks a
            flavour pin, or has a ``[grch38_fasta]`` table missing one of its keys.
    """
    try:
        raw = tomllib.loads(pins_toml.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError, UnicodeDecodeError) as exc:
        raise RunTestsError(Exit.USAGE, f"cannot read {pins_toml}: {exc}") from exc
    datasets: dict[Flavour, DatasetPin] = {}
    for flavour in Flavour:
        table = raw.get(flavour.pin_name)
        if not isinstance(table, dict) or table.get("role") != "dataset":
            raise RunTestsError(
                Exit.USAGE,
                f'{pins_toml}: no [{flavour.pin_name}] pin with role = "dataset"',
            )
        datasets[flavour] = DatasetPin.from_table(flavour.pin_name, table)
    fasta: FastaPin | None = None
    table = raw.get(FASTA_PIN)
    if isinstance(table, dict):
        try:
            sha = str(table["sha"])
            fasta = FastaPin(
                url=str(table["repo"]).rstrip("/") + "/" + str(table["ref"]),
                sha256_fa=sha,
                ensembl_sum=str(table["ensembl_sum"]),
            )
        except KeyError as exc:
            raise RunTestsError(
                Exit.USAGE, f"{pins_toml}: [{FASTA_PIN}] has no {exc.args[0]!r} key"
            ) from exc
        if not re.fullmatch(r"[0-9a-f]{64}", sha):
            raise RunTestsError(
                Exit.USAGE,
                f"{pins_toml}: [{FASTA_PIN}].sha {sha!r} is not a 64-hex sha256 of the "
                ".fa — the end-to-end FASTA check cannot be skipped",
            )
    return datasets, fasta


@dataclass(frozen=True, slots=True, kw_only=True)
class RemoteFile:
    """One file of a dataset at the pinned revision, as the Hub describes it."""

    path: str
    size: int
    sha256: str | None
    """LFS digest; ``None`` for files stored in git (README, manifests)."""
    git_blob_sha: str | None = None
    """Git blob SHA-1 for metadata; trimmed manifests are checked structurally."""


class Lister(Protocol):
    """Lists a dataset at a revision. Injected so tests never touch the network."""

    def __call__(self, repo_id: str, revision: str) -> list[RemoteFile]:
        """Every file of ``repo_id`` at ``revision`` with its size and LFS digest."""
        ...


class Downloader(Protocol):
    """Materialises the files matching ``allow_patterns`` under ``local_dir``.

    Contract: files already present are left alone, a trimmed ``chrom_manifest.json``
    included (``snapshot_download`` skips a present file whose download metadata names
    the same commit). Only the manifests-only call, with exactly
    ``["*/chrom_manifest.json"]``, forces the full manifests back from the Hub:
    :func:`fetch` makes it under the lock, immediately before trimming, whenever
    :func:`_manifests_stale` finds a manifest that misses a requested shard on disk,
    and on a whole-genome run whose record says ``manifests_trimmed`` (no trim then).
    """

    def __call__(
        self, repo_id: str, revision: str, allow_patterns: list[str], local_dir: Path
    ) -> None:
        """Fetch the matching files of ``repo_id``@``revision`` into ``local_dir``."""
        ...


def hub_lister(repo_id: str, revision: str) -> list[RemoteFile]:
    """The real lister: one ``dataset_info(files_metadata=True)`` call, ~1 s in all.

    ``list_repo_tree(recursive=True, expand=True)`` is the obvious alternative and was
    measured at 314 s over the same 7633 files (page size 50, rate-limited); this one
    request returns every sibling with its size and LFS sha256.

    Raises:
        RunTestsError: :attr:`Exit.REVISION` when the revision or repository is not
            found; :attr:`Exit.INCOMPLETE` when the Hub cannot be reached.
    """
    import httpx
    from huggingface_hub import HfApi
    from huggingface_hub.errors import (
        HfHubHTTPError,
        RepositoryNotFoundError,
        RevisionNotFoundError,
    )

    try:
        info = HfApi().dataset_info(repo_id, revision=revision, files_metadata=True)
    except (RevisionNotFoundError, RepositoryNotFoundError) as exc:
        raise RunTestsError(Exit.REVISION, f"{repo_id}@{revision}: {exc}") from exc
    except (HfHubHTTPError, httpx.HTTPError, OSError) as exc:
        # httpx.ConnectError & co. are NOT OSError (httpx 0.28) but httpx.HTTPError.
        raise RunTestsError(
            Exit.INCOMPLETE, f"{repo_id}@{revision}: Hub unreachable: {exc}"
        ) from exc
    if info.sha != revision:
        raise RunTestsError(
            Exit.REVISION, f"{repo_id}: asked for {revision}, Hub answered {info.sha}"
        )
    return [
        RemoteFile(
            path=s.rfilename,
            size=int(s.size or 0),
            sha256=s.lfs.sha256 if s.lfs is not None else None,
            git_blob_sha=getattr(s, "blob_id", None),
        )
        for s in info.siblings or ()
    ]


def hub_downloader(
    repo_id: str,
    revision: str,
    allow_patterns: list[str],
    local_dir: Path,
    *,
    max_workers: int = 8,
) -> None:
    """The real downloader: ``snapshot_download`` pinned to ``revision``.

    Files already present and matching the ``local_dir/.cache/huggingface`` metadata are
    not fetched again, which is what makes a second run cheap. ``max_workers`` is
    ``snapshot_download``'s thread count: beyond 8 threads XET gains
    nothing and the Hub is loaded.

    Raises:
        RunTestsError: :attr:`Exit.REVISION` / :attr:`Exit.INCOMPLETE` mapped from the
            library's own exception classes.
    """
    import httpx
    from huggingface_hub import snapshot_download
    from huggingface_hub.errors import (
        HfHubHTTPError,
        IncompleteSnapshotError,
        RepositoryNotFoundError,
        RevisionNotFoundError,
    )

    manifests_only = allow_patterns == [f"*/{MANIFEST}"]
    try:
        # The Downloader contract: present files, trimmed manifests included, are left
        # alone; only the manifests-only call forces them back. fetch() asks for the
        # manifests alone, under the lock, right before it trims them — so a full
        # manifest can never be overwritten by another run's stale trim between its
        # re-fetch and its own trim.
        snapshot_download(
            repo_id,
            repo_type="dataset",
            revision=revision,
            allow_patterns=allow_patterns,
            local_dir=local_dir,
            force_download=manifests_only,
            max_workers=max_workers,
        )
    except (RevisionNotFoundError, RepositoryNotFoundError) as exc:
        raise RunTestsError(Exit.REVISION, f"{repo_id}@{revision}: {exc}") from exc
    except IncompleteSnapshotError as exc:
        raise RunTestsError(Exit.INCOMPLETE, f"{repo_id}@{revision}: {exc}") from exc
    except (HfHubHTTPError, httpx.HTTPError, OSError) as exc:
        raise RunTestsError(
            Exit.INCOMPLETE, f"{repo_id}@{revision}: Hub unreachable: {exc}"
        ) from exc
    except ValueError as exc:
        # huggingface_hub re-raises a transport failure of a forced download as
        # ``ValueError("Force download failed due to the above error.")`` chained from
        # the httpx exception (measured 2026-09-07: ``httpx.ReadTimeout`` escaped as a
        # raw traceback, exit 1). Same class of failure: exit 4.
        cause = exc.__cause__ or exc.__context__
        if isinstance(cause, (HfHubHTTPError, httpx.HTTPError, OSError)):
            raise RunTestsError(
                Exit.INCOMPLETE,
                f"{repo_id}@{revision}: Hub unreachable: {cause} ({exc})",
            ) from exc
        raise


@dataclass(frozen=True, slots=True, kw_only=True)
class Selection:
    """What one run asks for."""

    root: Path
    flavours: tuple[Flavour, ...]
    contigs: tuple[str, ...] | None
    """``None`` means the whole genome."""
    fasta: bool
    trim_manifests: bool
    verify: bool
    dry_run: bool


def allow_patterns(contigs: Sequence[str] | None) -> list[str]:
    """The ``allow_patterns`` for one flavour.

    Whole genome is ``["*"]``; per-contig is every ``<entity>/<contig>.parquet`` plus
    every manifest, README and cache metadata. Omitting chromosome synonyms breaks
    RefSeq accession lookup even when the chromosome's Parquet shard is present.
    """
    if contigs is None:
        return ["*"]
    return [f"*/{c}.parquet" for c in contigs] + [
        f"*/{MANIFEST}",
        README,
        *CACHE_METADATA,
    ]


def select_remote(
    files: Iterable[RemoteFile], patterns: Sequence[str]
) -> list[RemoteFile]:
    """The remote files ``patterns`` match, with :mod:`fnmatch`.

    The same rule ``huggingface_hub.utils.filter_repo_objects`` applies inside
    ``snapshot_download``.
    """
    return [f for f in files if any(fnmatch.fnmatch(f.path, p) for p in patterns)]


@dataclass(slots=True, kw_only=True)
class FlavourRecord:
    """One flavour's entry in ``PROVENANCE.json``.

    Every field describes the ROOT as it stands after the run, not the run itself:
    ``contigs`` accumulates across runs (``"ALL"`` once a whole-genome run happened),
    and ``files`` / ``bytes`` count every file under ``116_GRCh38_<flavour>/`` on disk
    (``.cache/`` excluded). Per-run numbers live in :class:`Run`.
    """

    repo_id: str
    revision: str
    contigs: list[str] | str
    manifests_trimmed: bool
    files: int
    """Files of this flavour on disk after the run (cumulative, like ``contigs``)."""
    bytes: int
    """Their byte total on disk after the run."""


@dataclass(slots=True, kw_only=True)
class FastaRecord:
    """The FASTA entry in ``PROVENANCE.json``."""

    url: str
    ensembl_sum: str
    sha256_fa: str


@dataclass(slots=True, kw_only=True)
class Run:
    """One invocation, appended to ``PROVENANCE.json`` ``runs``."""

    at: str
    argv: list[str]
    added_files: int
    skipped_files: int
    refreshed_manifests: int


@dataclass(slots=True, kw_only=True)
class Provenance:
    """The cache datasets, FASTA and run history in ``PROVENANCE.json``."""

    schema_version: int = SCHEMA_VERSION
    written_at: str = ""
    tool: str = ""
    pins_toml_sha256: str = ""
    datasets: dict[str, FlavourRecord] = field(default_factory=dict)
    fasta: FastaRecord | None = None
    runs: list[Run] = field(default_factory=list)

    def to_json(self) -> str:
        """Stable, indented JSON with a trailing newline."""
        return json.dumps(asdict(self), indent=2, sort_keys=False) + "\n"

    @classmethod
    def from_json(cls, text: str) -> Provenance:
        """Inverse of :meth:`to_json`.

        Raises:
            RunTestsError: :attr:`Exit.USAGE` for corrupt JSON, a key no dataclass here
                knows, a missing key, or a ``schema_version`` other than
                :data:`SCHEMA_VERSION` — the file is unreadable, not a wrong revision.
        """
        try:
            raw = json.loads(text)
        except json.JSONDecodeError as exc:
            raise RunTestsError(
                Exit.USAGE, f"{PROVENANCE}: not valid JSON: {exc}"
            ) from exc
        if not isinstance(raw, dict):
            raise RunTestsError(Exit.USAGE, f"{PROVENANCE}: top level is not an object")
        if raw.get("schema_version") != SCHEMA_VERSION:
            raise RunTestsError(
                Exit.USAGE,
                f"{PROVENANCE}: schema_version {raw.get('schema_version')!r} "
                f"is not {SCHEMA_VERSION}; this tool cannot read it",
            )
        fasta = raw.get("fasta")
        try:
            return cls(
                schema_version=SCHEMA_VERSION,
                written_at=raw.get("written_at", ""),
                tool=raw.get("tool", ""),
                pins_toml_sha256=raw.get("pins_toml_sha256", ""),
                datasets={
                    k: FlavourRecord(**v) for k, v in raw.get("datasets", {}).items()
                },
                fasta=FastaRecord(**fasta) if fasta else None,
                runs=[Run(**r) for r in raw.get("runs", [])],
            )
        except (TypeError, KeyError, AttributeError) as exc:
            raise RunTestsError(
                Exit.USAGE,
                f"{PROVENANCE}: does not match schema_version {SCHEMA_VERSION}: {exc}",
            ) from exc


def read_provenance(root: Path) -> Provenance | None:
    """The root's ``PROVENANCE.json``, or ``None`` when the root is fresh."""
    path = root / PROVENANCE
    if not path.is_file():
        return None
    return Provenance.from_json(path.read_text(encoding="utf-8"))


def _atomic_write_text(path: Path, text: str) -> None:
    """Write ``text`` to ``path`` via a process-unique temp file and ``os.replace``.

    A reader never sees a half-written file, and two processes writing the same path
    never share a temp name (a fixed ``.tmp`` name made ``os.replace`` fail with
    ``FileNotFoundError`` in the other process).
    """
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        tmp.write_text(text, encoding="utf-8")
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def write_provenance(root: Path, provenance: Provenance) -> None:
    """Write atomically (:func:`_atomic_write_text`).

    Atomic against readers; writers must additionally hold :func:`provenance_lock`, or
    two runs overwrite each other's records.
    """
    _atomic_write_text(root / PROVENANCE, provenance.to_json())


@contextmanager
def provenance_lock(root: Path) -> Iterator[None]:
    """Exclusive ``fcntl.flock`` on ``<root>/PROVENANCE.lock`` around read-modify-write.

    Two runs on one root (two campaigns sharing ``/mnt/hf-cache/cache``) each read the
    file at their start and write the whole object at their end; without the lock the
    slower one overwrites the faster one's record. The lock is held only around the
    final re-read + merge + write, never across a download.
    """
    root.mkdir(parents=True, exist_ok=True)
    with (root / PROVENANCE_LOCK).open("a+") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _sha256_file(path: Path, progress: Progress | None = None) -> str:
    """Hex sha256 of ``path``, streamed in 1 MiB chunks."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(_CHUNK):
            digest.update(chunk)
            if progress is not None:
                progress.update(handle.tell())
    return digest.hexdigest()


def trim_manifest(path: Path) -> bool:
    """Keep only the manifest entries whose ``dataset`` file exists beside it.

    Order is preserved and shards are never touched. Returns ``True`` when the file was
    rewritten — atomically, so a concurrent run reading the manifest never sees a torn
    file; callers hold :func:`provenance_lock` around the trim loop as well, so one run
    cannot trim a manifest another is about to re-download.

    Raises:
        RunTestsError: :attr:`Exit.INCOMPLETE` when no entry's shard is on disk. A
            manifest is never emptied and never left naming shards that were not
            fetched: an entity without a single requested shard is refused by
            :func:`plan` before
            any download, and this is the guard behind it. (For ``variation`` an empty
            manifest is a hard error in the reader — ``cache_source.rs`` — as well.)
    """
    entries = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(entries, list):
        raise RunTestsError(Exit.VERIFY, f"{path}: not a JSON list of entries")
    kept = [e for e in entries if (path.parent / str(e["dataset"])).is_file()]
    if not kept:
        raise RunTestsError(
            Exit.INCOMPLETE,
            f"{path.parent.name}/{path.name}: no shard of any requested contig is on "
            f"disk, so the manifest cannot be trimmed ({len(entries)} entries, first "
            f"{entries[0].get('dataset')!r}); an entity without shards is not a usable "
            "cache",
        )
    if len(kept) == len(entries):
        return False
    _atomic_write_text(path, json.dumps(kept, indent=2) + "\n")
    return True


def _recorded_trimmed(existing: Provenance | None, p: Plan) -> bool:
    """Whether ``PROVENANCE.json`` records ``p``'s flavour with trimmed manifests.

    Args:
        existing: The record read at the start of the run, ``None`` on a fresh root.
        p: The flavour's plan.

    Returns:
        ``True`` only when the flavour has a record and it says ``manifests_trimmed``.
    """
    record = existing.datasets.get(p.flavour.value) if existing else None
    return record is not None and record.manifests_trimmed


def _manifests_stale(flavour_dir: Path, requested: frozenset[str]) -> bool:
    """True when some manifest of ``flavour_dir`` misses a requested shard or a file.

    The trim gate: it answers "must the manifests be re-fetched and trimmed?" from the
    directory alone, so a run trims whenever the root needs it and not only when it
    downloaded something. A manifest is stale when it names a shard that is not beside
    it (an interrupted run, or ``--no-trim-manifests`` followed by a plain one), when a
    shard THIS run requested (``requested``, paths relative to ``flavour_dir``) is on
    disk beside it but not listed (a top-up with another contig: the downloader leaves
    the present, already trimmed manifest alone, #206), or when it is unreadable.
    Re-materialising from the Hub and trimming repairs all three.

    Only requested shards are checked in that direction: a shard the Hub's own
    manifest omits (e.g. ``exon/GL000009.2.parquet`` of a whole-flavour download)
    can never become listed, so checking every ``*.parquet`` would keep the gate
    open and force a manifests-only Hub call on every run. A root consistent for the
    requested shards is left untouched, so a run that changes nothing rewrites no
    manifest.
    """
    for entity in ENTITIES:
        manifest = flavour_dir / entity / MANIFEST
        if not manifest.is_file():
            continue
        try:
            entries = json.loads(manifest.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return True
        if not isinstance(entries, list):
            return True
        listed = {
            str(entry["dataset"])
            for entry in entries
            if isinstance(entry, dict) and "dataset" in entry
        }
        if any(not (manifest.parent / name).is_file() for name in listed):
            return True
        unlisted = {
            name
            for path in requested
            if (parts := path.split("/"))[0] == entity
            and len(parts) == 2
            and (name := parts[1]).endswith(".parquet")
            and (manifest.parent / name).is_file()
        } - listed
        if unlisted:
            return True
    return False


def _on_disk(flavour_dir: Path) -> set[str]:
    """Relative paths of every regular file under a flavour dir.

    Excluded, because they are transient and a concurrent run's may vanish under our
    feet: ``.cache/`` (snapshot_download's metadata and in-flight downloads) and this
    tool's own ``.<name>.<pid>.tmp`` (:data:`_TRANSIENT_NAME`). Nothing else — a
    dot-file the Hub lists (``.gitattributes``) is part of the selection and hiding it
    made every idempotent whole-genome run exit 4.
    """
    if not flavour_dir.is_dir():
        return set()
    return {
        p.relative_to(flavour_dir).as_posix()
        for p in flavour_dir.rglob("*")
        if p.is_file()
        and ".cache" not in p.relative_to(flavour_dir).parts
        and not _TRANSIENT_NAME.match(p.name)
    }


def _bytes_on_disk(flavour_dir: Path, rel_paths: Iterable[str]) -> int:
    """Byte total of ``rel_paths`` under ``flavour_dir``; a file gone meanwhile is 0."""
    total = 0
    for rel in rel_paths:
        try:
            total += (flavour_dir / rel).stat().st_size
        except FileNotFoundError:
            continue
    return total


def verify_flavour(
    flavour_dir: Path,
    wanted: Sequence[RemoteFile],
    *,
    out: Callable[[str], None] | None = None,
) -> list[str]:
    """Compare every wanted shard's sha256 with the Hub's LFS digest.

    Manifests carry no LFS digest (and may have been trimmed), so they are checked
    structurally instead: every ``dataset`` they name must be on disk.

    Returns:
        Violation lines; empty means verified.
    """
    violations: list[str] = []
    progress = Progress(
        f"Verifying {flavour_dir.name}", len(wanted), unit="files", out=out
    )
    for index, remote in enumerate(wanted):
        progress.update(index)
        local = flavour_dir / remote.path
        if not local.is_file():
            violations.append(f"missing: {remote.path}")
            continue
        if remote.sha256 is not None:
            actual = _sha256_file(local)
            if actual != remote.sha256:
                violations.append(
                    f"sha256 mismatch: {remote.path} is {actual}, "
                    f"Hub says {remote.sha256}"
                )
        elif remote.path.endswith(MANIFEST):
            for entry in json.loads(local.read_text(encoding="utf-8")):
                if not (local.parent / str(entry["dataset"])).is_file():
                    violations.append(
                        f"manifest {remote.path} names absent shard {entry['dataset']}"
                    )
        elif remote.git_blob_sha is not None:
            digest = hashlib.sha1(f"blob {local.stat().st_size}\0".encode())
            with local.open("rb") as handle:
                for chunk in iter(lambda: handle.read(_CHUNK), b""):
                    digest.update(chunk)
            if digest.hexdigest() != remote.git_blob_sha:
                violations.append(f"git blob mismatch: {remote.path}")
    progress.update(len(wanted))
    return violations


@dataclass(frozen=True, slots=True, kw_only=True)
class Plan:
    """What the run will do for one flavour, before touching the disk."""

    flavour: Flavour
    pin: DatasetPin
    patterns: list[str]
    wanted: list[RemoteFile]

    @property
    def bytes(self) -> int:
        """Byte total of the wanted files."""
        return sum(f.size for f in self.wanted)


def plan(
    selection: Selection, pins: dict[Flavour, DatasetPin], lister: Lister
) -> list[Plan]:
    """List each selected flavour on the Hub and keep what the selection names.

    Raises:
        RunTestsError: :attr:`Exit.INCOMPLETE` when a flavour's selection names no
            file — an unknown contig must not produce a green empty run — or when some
            entity of the flavour has no shard for any requested contig (``motif`` and
            ``regulatory`` carry chr1-22/X/Y only, so ``chrMT`` alone is refused):
            such a root would keep a manifest naming shards that were never fetched.
    """
    plans: list[Plan] = []
    for flavour in selection.flavours:
        pin = pins[flavour]
        patterns = allow_patterns(selection.contigs)
        wanted = select_remote(lister(pin.repo_id, pin.revision), patterns)
        shards = [f for f in wanted if f.path.endswith(".parquet")]
        if not shards:
            raise RunTestsError(
                Exit.INCOMPLETE,
                f"{flavour.dir_name}: selection {patterns} names no shard "
                f"at {pin.revision[:12]} "
                f"— check required_contigs (names are chr21, chrX, chrY, ...)",
            )
        if selection.contigs is not None:
            shard_dirs = {f.path.split("/", 1)[0] for f in shards}
            bare = [e for e in ENTITIES if e not in shard_dirs]
            if bare:
                raise RunTestsError(
                    Exit.INCOMPLETE,
                    f"{flavour.dir_name}: no shard for {list(selection.contigs)} in "
                    f"{bare} at {pin.revision[:12]} — these entities do not carry "
                    "the requested contig(s); such a root would keep a manifest naming "
                    "shards that were never fetched. Pick contigs every entity "
                    "carries (chr1-22, chrX, chrY) or fetch the whole genome",
                )
        missing_metadata = sorted(set(CACHE_METADATA) - {f.path for f in wanted})
        if missing_metadata:
            raise RunTestsError(
                Exit.INCOMPLETE,
                f"{flavour.dir_name}@{pin.revision[:12]} "
                "lacks required cache metadata: "
                f"{', '.join(missing_metadata)}",
            )
        plans.append(Plan(flavour=flavour, pin=pin, patterns=patterns, wanted=wanted))
    return plans


def _check_revision_on_disk(existing: Provenance | None, p: Plan, root: Path) -> None:
    """Refuse to mix revisions: the root's record of this flavour must equal the pin."""
    if existing is None:
        return
    record = existing.datasets.get(p.flavour.value)
    if record is not None and record.revision != p.pin.revision:
        raise RunTestsError(
            Exit.REVISION,
            f"{p.flavour.dir_name}: revision on disk {record.revision} "
            f"!= PINS {p.pin.revision} "
            f"({p.flavour.pin_name}). One root holds one revision per flavour: fetch "
            "the pinned revision into a NEW root (set VEPYR_CACHE_ROOT) "
            f"and keep {root} "
            "as it is; do not "
            "delete PROVENANCE.json — that erases the audit trail and disarms this "
            "guard",
        )


def _merge_contigs(
    old: list[str] | str | None, new: Sequence[str] | None
) -> list[str] | str:
    """Fold a run's contigs into the record; ``ALL`` absorbs everything."""
    if new is None:
        return "ALL"
    if old == "ALL":
        return "ALL"
    merged = list(old or [])
    merged += [c for c in new if c not in merged]
    return merged


def fetch(
    selection: Selection,
    pins: dict[Flavour, DatasetPin],
    fasta_pin: FastaPin | None,
    *,
    lister: Lister,
    downloader: Downloader,
    fasta_fetcher: Callable[[str, Path], None] | None = None,
    argv: Sequence[str] = (),
    pins_toml: Path | None = None,
    tool: str = "tools/run_tests/fetch.py",
    out: Callable[[str], None] = print,
) -> Exit:
    """Run one selection end to end. Pure of I/O apart from the injected callables.

    The downloader is called only for a flavour with at least one requested file
    absent: a complete directory downloads nothing, a partial one only what is missing —
    presence is by path; ``--verify`` is what checks digests.
    Trimming is driven by the directory, not by the download: a per-contig run
    re-fetches and trims whenever a manifest names an absent shard or misses a
    requested one on disk (:func:`_manifests_stale`), so it is idempotent and repairs
    a root left untrimmed by an interrupted or ``--no-trim-manifests`` run, or left
    stale by a top-up. A whole-genome run whose flavour ``PROVENANCE.json`` records
    ``manifests_trimmed`` re-fetches the full manifests (no trim), so the root matches
    the ``contigs: ALL`` it records (#223).

    Returns:
        :attr:`Exit.OK` on success; every failure is
        raised as :class:`RunTestsError` by the helpers and converted by
        :mod:`run_tests.cli`.
    """
    plans = plan(selection, pins, lister)
    if selection.dry_run:
        total = 0
        for p in plans:
            for f in sorted(p.wanted, key=lambda f: f.path):
                out(f"{p.flavour.dir_name}/{f.path}\t{f.size}")
            total += p.bytes
            out(
                f"# {p.flavour.dir_name}: {len(p.wanted)} files, {p.bytes} bytes "
                f"@ {p.pin.revision[:12]}"
            )
        out(
            f"# total: {sum(len(p.wanted) for p in plans)} files, {total} bytes; "
            "nothing written"
        )
        return Exit.OK

    root = selection.root
    root.mkdir(parents=True, exist_ok=True)
    with provenance_lock(root):
        existing = read_provenance(root)
    for p in plans:
        _check_revision_on_disk(existing, p, root)

    records: dict[str, tuple[Plan, set[str]]] = {}
    added = skipped = refreshed = 0
    for p in plans:
        flavour_dir = root / p.flavour.dir_name
        before = _on_disk(flavour_dir)
        wanted_paths = {f.path for f in p.wanted}
        if wanted_paths - before:
            downloader(p.pin.repo_id, p.pin.revision, p.patterns, flavour_dir)
        after = _on_disk(flavour_dir)
        missing = sorted(wanted_paths - after)
        if missing:
            raise RunTestsError(
                Exit.INCOMPLETE,
                f"{p.flavour.dir_name}: {len(missing)} of {len(wanted_paths)} "
                "requested files "
                f"missing after download, first: {missing[0]}",
            )
        if (
            selection.contigs is not None
            and selection.trim_manifests
            and _manifests_stale(flavour_dir, frozenset(wanted_paths))
        ):
            with provenance_lock(root):
                # Re-fetch the full manifests INSIDE the lock, then trim to the shards
                # on disk. Outside the lock, another run's trim (from a read that
                # predates this run's shards) could land between the re-fetch and the
                # trim and drop this run's contig for good — trimming never re-adds.
                downloader(
                    p.pin.repo_id, p.pin.revision, [f"*/{MANIFEST}"], flavour_dir
                )
                for entity in ENTITIES:
                    manifest = flavour_dir / entity / MANIFEST
                    if manifest.is_file() and trim_manifest(manifest):
                        refreshed += 1
        elif selection.contigs is None and _recorded_trimmed(existing, p):
            with provenance_lock(root):
                # A whole-genome run on a root that a per-contig run trimmed: restore
                # the full manifests (no trim), so they match the ``contigs: ALL`` this
                # run records. Keyed on the record, not on the disk, so a shard the Hub
                # manifest itself omits never triggers the forced call (#223).
                downloader(
                    p.pin.repo_id, p.pin.revision, [f"*/{MANIFEST}"], flavour_dir
                )
                refreshed += sum(
                    (flavour_dir / entity / MANIFEST).is_file() for entity in ENTITIES
                )
        added += len((after - before) & wanted_paths)
        skipped += len(before & wanted_paths)
        if selection.verify:
            violations = verify_flavour(flavour_dir, p.wanted, out=out)
            if violations:
                raise RunTestsError(
                    Exit.VERIFY,
                    f"{p.flavour.dir_name}: {len(violations)} violation(s); "
                    f"first: {violations[0]}",
                )
        records[p.flavour.value] = (p, after)

    fasta_record: FastaRecord | None = None
    if selection.fasta:
        if fasta_pin is None:
            raise RunTestsError(
                Exit.USAGE,
                f"FASTA requested but PINS.toml has no [{FASTA_PIN}] pin",
            )
        if fasta_fetcher is None:
            raise RunTestsError(
                Exit.USAGE, "FASTA requested but no fetcher was wired in"
            )
        fasta_record = fetch_fasta(root / FASTA_DIR, fasta_pin, fasta_fetcher, out=out)

    # Read-modify-write under the lock, against the file as it is NOW — another run
    # may have committed its own flavour while this one was downloading.
    with provenance_lock(root):
        provenance = read_provenance(root) or Provenance()
        for name, (p, after) in records.items():
            _check_revision_on_disk(provenance, p, root)
            old = provenance.datasets.get(name)
            flavour_dir = root / p.flavour.dir_name
            provenance.datasets[name] = FlavourRecord(
                repo_id=p.pin.repo_id,
                revision=p.pin.revision,
                contigs=_merge_contigs(old.contigs if old else None, selection.contigs),
                manifests_trimmed=selection.contigs is not None
                and selection.trim_manifests,
                files=len(after),
                bytes=_bytes_on_disk(flavour_dir, after),
            )
        if fasta_record is not None:
            provenance.fasta = fasta_record
        provenance.written_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        provenance.tool = tool
        if pins_toml is not None:
            provenance.pins_toml_sha256 = _sha256_file(pins_toml)
        provenance.runs.append(
            Run(
                at=provenance.written_at,
                argv=list(argv),
                added_files=added,
                skipped_files=skipped,
                refreshed_manifests=refreshed,
            )
        )
        write_provenance(root, provenance)
    out(
        f"ok: {added} added, {skipped} already present, "
        f"{refreshed} manifest(s) refreshed -> {root / PROVENANCE}"
    )
    return Exit.OK


# --------------------------------------------------------------------------------------
# FASTA
# --------------------------------------------------------------------------------------


def bsd_sum(path: Path, progress: Progress | None = None) -> tuple[int, int]:
    """The BSD ``sum`` checksum Ensembl's ``CHECKSUMS`` use: ``(checksum, KiB blocks)``.

    Algorithm as in coreutils ``sum`` (default mode): 16-bit accumulator, rotated right
    by one before each byte is added; block count rounds up to 1024-byte blocks.
    """
    checksum = 0
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(_CHUNK):
            size += len(chunk)
            for byte in chunk:
                checksum = ((checksum >> 1) | ((checksum & 1) << 15)) + byte
                checksum &= 0xFFFF
            if progress is not None:
                progress.update(size)
    return checksum, (size + 1023) // 1024


def write_fai(fa: Path, progress: Progress | None = None) -> Path:
    """Write ``<fa>.fai`` in the htslib format.

    Columns: name, length, offset, line bases, line bytes. Pure Python so the tool needs
    no ``samtools``; a one-off ``samtools faidx`` on the VM is the positive control
    recorded in the command ledger.
    """
    fai = fa.with_name(fa.name + ".fai")
    lines: list[str] = []
    name: str | None = None
    length = offset = line_bases = line_bytes = 0
    with fa.open("rb") as handle:
        position = 0
        next_report = _CHUNK
        for raw in handle:
            if progress is not None and position >= next_report:
                progress.update(position)
                next_report = position + _CHUNK
            if raw.startswith(b">"):
                if name is not None:
                    lines.append(
                        f"{name}\t{length}\t{offset}\t{line_bases}\t{line_bytes}"
                    )
                name = raw[1:].split()[0].decode()
                position += len(raw)
                offset = position
                length = line_bases = line_bytes = 0
                continue
            stripped = raw.rstrip(b"\r\n")
            if line_bases == 0:
                line_bases, line_bytes = len(stripped), len(raw)
            length += len(stripped)
            position += len(raw)
    if name is not None:
        lines.append(f"{name}\t{length}\t{offset}\t{line_bases}\t{line_bytes}")
    fai.write_text("\n".join(lines) + "\n", encoding="utf-8")
    if progress is not None:
        progress.update(position)
    return fai


def url_fetcher(url: str, destination: Path) -> None:
    """Stream ``url`` to ``destination`` with :mod:`urllib` (Ensembl FTP over HTTPS)."""
    from urllib.request import urlopen

    progress = Progress("Downloading reference FASTA", None)
    with urlopen(url) as response, destination.open("wb") as handle:
        length = response.headers.get("Content-Length", "")
        progress.total = int(length) if length.isdigit() else None
        progress.update(0, force=True)
        while chunk := response.read(_CHUNK):
            handle.write(chunk)
            progress.update(handle.tell())
    progress.finish()


def fetch_fasta(
    fasta_dir: Path,
    pin: FastaPin,
    fetcher: Callable[[str, Path], None],
    *,
    out: Callable[[str], None] | None = None,
) -> FastaRecord:
    """Fetch the ``.fa.gz`` once, check its BSD sum, gunzip, index, sha256 the ``.fa``.

    Idempotent: an existing ``.fa`` whose sha256 matches the pin is left alone. The
    ``.gz`` is fetched into ``<name>.part`` and renamed only when the fetcher returns;
    a dropped transfer leaves nothing behind, so the next run fetches again (no retry
    inside one run — a re-pull of a file the server may have changed is not silent
    business).

    Raises:
        RunTestsError: :attr:`Exit.VERIFY` when the sum or the sha256 disagrees with the
            pin (the sum message names the ``.gz`` path to remove);
            :attr:`Exit.INCOMPLETE` when the transfer fails (``OSError``, which
            includes ``urllib.error.URLError``) — never a bare traceback.
    """
    fasta_dir.mkdir(parents=True, exist_ok=True)
    fa = fasta_dir / pin.fa_name
    gz = fasta_dir / pin.gz_name
    if fa.is_file() and (fa.with_name(fa.name + ".fai")).is_file():
        digest = _sha256_file(
            fa,
            Progress("Verifying reference FASTA SHA-256", fa.stat().st_size, out=out),
        )
        if digest == pin.sha256_fa:
            return FastaRecord(
                url=pin.url, ensembl_sum=pin.ensembl_sum, sha256_fa=digest
            )
    if not gz.is_file():
        part = gz.with_name(gz.name + ".part")
        try:
            fetcher(pin.url, part)
        except OSError as exc:
            part.unlink(missing_ok=True)
            raise RunTestsError(
                Exit.INCOMPLETE, f"{pin.url}: transfer failed ({exc}); nothing kept"
            ) from exc
        except BaseException:
            part.unlink(missing_ok=True)
            raise
        os.replace(part, gz)
    checksum, blocks = bsd_sum(
        gz, Progress("Checking FASTA archive checksum", gz.stat().st_size, out=out)
    )
    if f"{checksum} {blocks}" != pin.ensembl_sum:
        raise RunTestsError(
            Exit.VERIFY,
            f"{gz}: sum is '{checksum} {blocks}', Ensembl CHECKSUMS says "
            f"'{pin.ensembl_sum}' — remove that file to download it again",
        )
    progress = Progress("Decompressing reference FASTA", gz.stat().st_size, out=out)
    with gz.open("rb") as compressed, fa.open("wb") as target:
        with gzip.GzipFile(fileobj=compressed) as source:
            while chunk := source.read(_CHUNK):
                target.write(chunk)
                progress.update(compressed.tell())
    progress.finish()
    write_fai(fa, Progress("Indexing reference FASTA", fa.stat().st_size, out=out))
    digest = _sha256_file(
        fa, Progress("Verifying reference FASTA SHA-256", fa.stat().st_size, out=out)
    )
    if digest != pin.sha256_fa:
        raise RunTestsError(
            Exit.VERIFY,
            f"{fa.name}: sha256 {digest} "
            f"!= PINS.toml [{FASTA_PIN}].sha {pin.sha256_fa}",
        )
    return FastaRecord(url=pin.url, ensembl_sum=pin.ensembl_sum, sha256_fa=digest)
