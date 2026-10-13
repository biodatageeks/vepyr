"""Download published vepyr caches and plugin caches from Hugging Face."""

from __future__ import annotations

import contextlib
from collections.abc import Iterator


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
        raise ValueError(f"{where} was not found on Hugging Face: {exc}") from exc
    except errors.LocalEntryNotFoundError as exc:
        raise RuntimeError(
            f"{where} is not available offline and could not be downloaded: {exc}"
        ) from exc
    except OSError as exc:
        raise RuntimeError(f"could not download {where}: {exc}") from exc
    except Exception as exc:
        if _is_transport_error(exc):
            raise RuntimeError(f"could not download {where}: {exc}") from exc
        raise
