"""Download published vepyr caches and plugin caches from Hugging Face."""

from __future__ import annotations

import contextlib
import os
import re
from collections.abc import Iterator

from vepyr import _published

_CORE_SIDECARS = ["*/chrom_manifest.json", "reference_policy.json", "chr_synonyms.txt"]


def _list_repo_files(repo_id: str, *, revision: str) -> list[str]:
    """File paths of a dataset repository at ``revision``."""
    from huggingface_hub import HfApi

    return HfApi().list_repo_files(repo_id, repo_type="dataset", revision=revision)


def _snapshot_download(
    repo_id: str, *, revision: str, local_dir: str, allow_patterns: list[str]
) -> str:
    """Fetch the matching files of a dataset repository into ``local_dir``."""
    from huggingface_hub import snapshot_download

    return snapshot_download(
        repo_id,
        repo_type="dataset",
        revision=revision,
        local_dir=local_dir,
        allow_patterns=allow_patterns,
    )


@contextlib.contextmanager
def _progress(show: bool) -> Iterator[None]:
    """Hide the library's progress bars for the block when ``show`` is false."""
    from huggingface_hub import utils

    if show:
        yield
        return
    already_disabled = utils.are_progress_bars_disabled()
    utils.disable_progress_bars()
    try:
        yield
    finally:
        if not already_disabled:
            utils.enable_progress_bars()


def _one_line(exc: BaseException) -> str:
    """The exception text with line breaks folded, for one-line CLI errors."""
    return " ".join(str(exc).split())


def _is_transport_error(exc: Exception) -> bool:
    # huggingface_hub 1.x uses httpx, 2.x httpx2; neither error is an OSError.
    return type(exc).__module__.split(".")[0] in ("httpx", "httpx2")


@contextlib.contextmanager
def _hub_errors(repo_id: str, revision: str) -> Iterator[None]:
    """Translate huggingface_hub exceptions into ValueError / RuntimeError."""
    from huggingface_hub import errors

    where = f"{repo_id} at revision {revision}"
    try:
        yield
    except (errors.RepositoryNotFoundError, errors.RevisionNotFoundError) as exc:
        raise ValueError(
            f"{where} was not found on Hugging Face: {_one_line(exc)}"
        ) from exc
    except errors.LocalEntryNotFoundError as exc:
        raise RuntimeError(
            f"{where} is not available offline and could not be downloaded: {_one_line(exc)}"
        ) from exc
    except OSError as exc:
        raise RuntimeError(f"could not download {where}: {_one_line(exc)}") from exc
    except Exception as exc:
        if _is_transport_error(exc):
            raise RuntimeError(f"could not download {where}: {_one_line(exc)}") from exc
        raise


def _natural_key(name: str) -> list:
    return [int(part) if part.isdigit() else part for part in re.split(r"(\d+)", name)]


def _select_contigs(
    requested: list[str] | None, available: set[str], where: str
) -> list[str] | None:
    """Map requested names to dataset contigs; ``None`` means all contigs.

    A name is matched as given; failing that, a name without a ``chr`` prefix
    is tried with one (``22`` -> ``chr22``, ``MT`` -> ``chrMT``). Duplicates
    after matching are dropped, keeping the first occurrence.
    """
    if requested is None:
        return None
    if not requested:
        raise ValueError("chroms must name at least one contig, or be None for all")
    selected: list[str] = []
    missing: list[str] = []
    for name in requested:
        if name in available:
            contig = name
        elif not name.startswith("chr") and f"chr{name}" in available:
            contig = f"chr{name}"
        else:
            missing.append(name)
            continue
        if contig not in selected:
            selected.append(contig)
    if missing:
        main = sorted((c for c in available if c.startswith("chr")), key=_natural_key)
        others = len(available) - len(main)
        listing = ", ".join(main) + (f" and {others} other contigs" if others else "")
        raise ValueError(
            f"{where} has no contig {', '.join(missing)}; available: {listing}"
        )
    return selected


def _shard_contigs(files: list[str], nested: bool) -> set[str]:
    """Contig names of the ``.parquet`` shards (``<entity>/<contig>`` if nested)."""
    depth = 2 if nested else 1
    return {
        name.rsplit("/", 1)[-1].removesuffix(".parquet")
        for name in files
        if name.endswith(".parquet") and name.count("/") == depth - 1
    }


def download_cache(
    release: int,
    cache_dir: str,
    *,
    cache_type: str,
    assembly: str = "GRCh38",
    chroms: list[str] | None = None,
    revision: str | None = None,
    show_progress: bool = True,
) -> str:
    """Download a published vepyr cache from Hugging Face.

    Parameters
    ----------
    release : int
        Ensembl release, e.g. ``116``.
    cache_dir : str
        Root directory; the cache is written to
        ``<cache_dir>/<release>_<assembly>_<cache_type>``, the layout
        :func:`build_cache` produces. ``~`` is expanded.
    cache_type : {"ensembl", "merged", "refseq"}
        Cache type.
    assembly : str
        Genome assembly (default: ``"GRCh38"``).
    chroms : list of str or None
        Contigs to download, e.g. ``["22"]`` or ``["chr22", "chrX"]``; a name
        without ``chr`` also matches the prefixed one. ``None`` downloads every
        contig. The per-entity manifests and the root sidecars are always
        included.
    revision : str or None
        Dataset revision. ``None`` uses the revision pinned by this vepyr
        release; any branch, tag or commit (e.g. ``"main"``) is passed on.
    show_progress : bool
        Show download progress bars (default: True).

    Returns
    -------
    str
        The cache directory, ready for ``annotate(cache_dir=...)``.

    Re-running resumes and skips files already present.
    """
    import vepyr

    vepyr._validate_cache_type(cache_type)
    repo_id = _published.cache_repo_id(release, assembly, cache_type)
    resolved = _published.cache_revision(release, assembly, cache_type, revision)
    where = f"{repo_id} at revision {resolved}"
    contigs = None
    if chroms is not None:
        if not chroms:
            _select_contigs(chroms, set(), where)
        with _hub_errors(repo_id, resolved):
            files = _list_repo_files(repo_id, revision=resolved)
        contigs = _select_contigs(chroms, _shard_contigs(files, nested=True), where)
    shards = ["*/*.parquet"] if contigs is None else [f"*/{c}.parquet" for c in contigs]
    target = vepyr._converted_cache_dir(
        os.path.expanduser(cache_dir), release, cache_type, assembly
    )
    with _progress(show_progress), _hub_errors(repo_id, resolved):
        _snapshot_download(
            repo_id,
            revision=resolved,
            local_dir=target,
            allow_patterns=[*shards, *_CORE_SIDECARS],
        )
    return target


_PLUGIN_NAME = re.compile(r"[A-Za-z0-9_-]+")


def download_plugin_cache(
    plugin: str,
    release: int,
    plugin_cache_root: str,
    *,
    assembly: str = "GRCh38",
    chroms: list[str] | None = None,
    revision: str | None = None,
    show_progress: bool = True,
) -> str:
    """Download a published vepyr plugin cache from Hugging Face.

    Parameters
    ----------
    plugin : str
        Plugin name, e.g. ``"clinvar"``. dbNSFP is supported by
        :func:`build_plugin_cache` but not published.
    release : int
        Ensembl release, e.g. ``116``.
    plugin_cache_root : str
        Root of the plugin cache tree; the cache is written to
        ``<plugin_cache_root>/plugin/<plugin>``. ``~`` is expanded.
    assembly : str
        Genome assembly (default: ``"GRCh38"``).
    chroms : list of str or None
        Contigs to download, as for :func:`download_cache`. The manifest is
        always included.
    revision : str or None
        Dataset revision; ``None`` uses the pinned one.
    show_progress : bool
        Show download progress bars (default: True).

    Returns
    -------
    str
        The plugin cache directory. Pass ``plugin_cache_root`` and
        ``plugins=[plugin]`` to :func:`annotate`.
    """
    if not _PLUGIN_NAME.fullmatch(plugin):
        raise ValueError(
            f"invalid plugin name {plugin!r}: use letters, digits, '_' or '-'"
        )
    repo_id = _published.plugin_repo_id(release, assembly, plugin)
    resolved = _published.plugin_revision(release, assembly, plugin, revision)
    where = f"{repo_id} at revision {resolved}"
    contigs = None
    if chroms is not None:
        if not chroms:
            _select_contigs(chroms, set(), where)
        with _hub_errors(repo_id, resolved):
            files = _list_repo_files(repo_id, revision=resolved)
        contigs = _select_contigs(chroms, _shard_contigs(files, nested=False), where)
    shards = ["*.parquet"] if contigs is None else [f"{c}.parquet" for c in contigs]
    target = os.path.join(os.path.expanduser(plugin_cache_root), "plugin", plugin)
    with _progress(show_progress), _hub_errors(repo_id, resolved):
        _snapshot_download(
            repo_id,
            revision=resolved,
            local_dir=target,
            allow_patterns=[*shards, "*.json"],
        )
    return target
