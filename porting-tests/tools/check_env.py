"""Check the local prerequisites of the repository tools (#170).

One command answers "can the repo tools run on this machine"::

    ./check_env [--vepyr-cache-root DIR] [--vep-cache-dir DIR] [--vep-fasta FILE]
                [--docker-timeout SECONDS]

Every check reuses the code of the tool that depends on it, so there is no
second copy of any rule:

``UV_PROJECT_ENVIRONMENT``
    Set, absolute and outside every git checkout (else ``uv``, ``./bless`` and
    the tools create a ``.venv`` inside the checkout).
``tool uv`` / ``tool git``
    On ``PATH``.
``bcftools pin``
    ``tools/normalize_input``'s own ``bcftools_versions()`` and
    ``check_pinned_version()``; the pin lives only there.
``docker daemon``
    ``bless.vep.require_docker`` with a wall-clock timeout.
``vepyr cache`` (with ``--vepyr-cache-root``)
    ``run_tests.tests.precheck_cache`` against ``PINS.toml``: provenance,
    pinned revisions and the pinned FASTA name; then the dataset directory
    (``fetch.Flavour.dir_name``, e.g. ``116_GRCh38_<flavour>``) of every flavour
    recorded in ``PROVENANCE.json`` must exist (owner decision on PR #181),
    and so must the dataset directory of the flavour the data-tests need
    (``REQUIRED_FLAVOUR``, ``merged``), whatever the
    provenance records (super-review F1 on PR #181).
``vep cache`` (with ``--vep-cache-dir``)
    ``bless.ensembl.require_complete_cache``.
``vep fasta`` (with ``--vep-fasta``)
    ``bless.ensembl.require_fasta`` plus ``<fasta>.fai`` present.

A cache check whose flag is absent prints ``SKIP``. One
``PASS``/``FAIL``/``SKIP <name>: <detail>`` line is printed per check, then a
summary line. Nothing is ever written; in particular the launcher runs with
``uv run --no-project``, so no project environment is created before the
``UV_PROJECT_ENVIRONMENT`` check can report it.

Exit codes: ``0`` every check passed (a ``SKIP`` neither passes nor fails);
``1`` a check failed; ``2`` the invocation environment is misconfigured
(``UV_PROJECT_ENVIRONMENT`` unset, relative or inside a git checkout) or bad
usage; ``3`` unexpected error.
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import shutil
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import IntEnum, StrEnum
from functools import cache
from importlib.machinery import SourceFileLoader
from pathlib import Path
from types import ModuleType
from typing import Final

from bless import BlessError
from bless.ensembl import require_complete_cache, require_fasta
from bless.vep import require_docker
from run_tests import fetch
from run_tests.tests import precheck_cache
from run_tests.verdict import Exit as RunTestsExit
from run_tests.verdict import RunTestsError

__all__ = [
    "Check",
    "Exit",
    "Status",
    "main",
    "run_checks",
    "uv_environment_check",
]

REPO: Final[Path] = Path(__file__).resolve().parent.parent
NORMALIZE_INPUT: Final[Path] = REPO / "tools" / "normalize_input"
PINS_TOML: Final[Path] = REPO / "PINS.toml"
UV_ENV: Final[str] = "UV_PROJECT_ENVIRONMENT"
TOOLS: Final[tuple[str, ...]] = ("uv", "git")
DEFAULT_DOCKER_TIMEOUT: Final[float] = 30.0
REQUIRED_FLAVOUR: Final[fetch.Flavour] = fetch.Flavour.MERGED
"""The flavour every data-test runs on (``dt``: ``FLAVOUR = "ensembl"``)."""


class Exit(IntEnum):
    """Process exit codes (see the module docstring)."""

    OK = 0
    FAIL = 1
    CONFIG = 2
    ERROR = 3


class Status(StrEnum):
    """Outcome of one check."""

    PASS = "PASS"
    FAIL = "FAIL"
    SKIP = "SKIP"


@dataclass(frozen=True, slots=True)
class Check:
    """One check result, rendered as ``<STATUS> <name>: <detail>``."""

    name: str
    status: Status
    detail: str

    def line(self) -> str:
        """The paste-ready output line."""
        return f"{self.status} {self.name}: {self.detail}"


def _outcome(name: str, probe: Callable[[], str], *errors: type[Exception]) -> Check:
    """Run ``probe``; its return value is the PASS detail, any of ``errors`` a FAIL.

    Args:
        name: Check name.
        probe: Callable doing the check and returning a detail string.
        *errors: Exception types that mean "check failed" (their message is the detail).

    Returns:
        The check result.
    """
    try:
        return Check(name, Status.PASS, probe())
    except errors as exc:
        return Check(name, Status.FAIL, str(exc))


def git_checkout_of(path: Path) -> Path | None:
    """The git checkout (or worktree) containing ``path``, else ``None``.

    Walks up from ``path`` looking for a ``.git`` entry (a directory in a clone,
    a file in a worktree or submodule); no ``git`` process is needed, so the
    check also works when ``git`` itself is missing.

    Args:
        path: An absolute path; it need not exist.

    Returns:
        The top directory of the enclosing checkout, or ``None``.
    """
    return next((p for p in (path, *path.parents) if (p / ".git").exists()), None)


def uv_environment_check(environ: Mapping[str, str]) -> Check:
    """``UV_PROJECT_ENVIRONMENT`` is set, absolute and outside every git checkout.

    Args:
        environ: The environment to read (``os.environ`` in production).

    Returns:
        The check result.
    """
    match environ.get(UV_ENV, ""):
        case "":
            return Check(
                UV_ENV,
                Status.FAIL,
                "unset: export it to an absolute path outside every "
                "checkout, else uv/./bless create .venv inside the checkout",
            )
        # No expanduser(): uv does not expand ``~``, so a literal ``~/x`` is
        # a relative path and uv would create ``./~/x`` inside the checkout.
        case text if not Path(text).is_absolute():
            return Check(
                UV_ENV,
                Status.FAIL,
                f"{text!r} is relative (uv does not expand '~'); "
                "use an absolute path outside every checkout",
            )
        case text:
            venv = Path(os.path.normpath(text))
            if (top := git_checkout_of(venv)) is not None:
                return Check(
                    UV_ENV,
                    Status.FAIL,
                    f"{venv} is inside git checkout {top}; "
                    "use a path outside every checkout",
                )
            return Check(UV_ENV, Status.PASS, str(venv))


@cache
def _normalize_input() -> ModuleType:
    """``tools/normalize_input`` loaded by path (it has no ``.py`` suffix).

    Returns:
        The executed module; ``__main__`` code does not run.
    """
    loader = SourceFileLoader("normalize_input", str(NORMALIZE_INPUT))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[loader.name] = module  # dataclasses resolves annotations via this
    loader.exec_module(module)
    return module


def bcftools_pin_check() -> Check:
    """The bcftools/htslib banner matches the pin of ``tools/normalize_input``."""
    ni = _normalize_input()

    def probe() -> str:
        found = ni.bcftools_versions()
        ni.check_pinned_version(found)
        return f"{found.bcftools} on {found.htslib} (pin of tools/normalize_input)"

    return _outcome("bcftools pin", probe, ni.NormalizeError)


def docker_check(timeout: float) -> Check:
    """The Docker daemon answers within ``timeout`` seconds."""
    return _outcome(
        "docker daemon",
        lambda: f"{require_docker(timeout=timeout)} answers",
        BlessError,
    )


def dataset_dirs(root: Path) -> list[Path]:
    """The dataset directory of every flavour recorded in ``root``'s provenance.

    Names come from :attr:`run_tests.fetch.Flavour.dir_name`, the same source the
    fetch writes them with; the flavours from ``PROVENANCE.json``, the same set
    ``precheck_cache`` checks when given no ``flavours``.

    Args:
        root: A cache root that already passed ``precheck_cache``.

    Returns:
        One path per recorded flavour, in provenance order.

    Raises:
        RunTestsError: Provenance vanished or names a flavour ``fetch`` does not know.
    """
    if (provenance := fetch.read_provenance(root)) is None:
        raise RunTestsError(
            RunTestsExit.INCOMPLETE, f"{root / fetch.PROVENANCE} missing"
        )
    try:
        return [root / fetch.Flavour(f).dir_name for f in provenance.datasets]
    except ValueError as exc:
        raise RunTestsError(
            RunTestsExit.USAGE, f"{root / fetch.PROVENANCE}: {exc}"
        ) from exc


def vepyr_cache_check(
    root: Path | None, required: fetch.Flavour = REQUIRED_FLAVOUR
) -> Check:
    """``root`` is a usable ``./run_tests`` cache for ``PINS.toml``.

    Args:
        root: The cache root, or ``None`` (check skipped).
        required: Flavour whose dataset directory must exist even when the
            provenance does not record it (the data-tests' flavour).

    Returns:
        The check result.
    """
    if root is None:
        return Check("vepyr cache", Status.SKIP, "no --vepyr-cache-root")

    def probe() -> str:
        precheck_cache(root, pins_toml=PINS_TOML, flavours=(required.value,))
        if not (need := root / required.dir_name).is_dir():
            raise RunTestsError(
                RunTestsExit.INCOMPLETE,
                f"dataset directory {need} missing (required: the data-tests "
                f"run on the {required.value} flavour, whatever "
                f"{fetch.PROVENANCE} records). "
                f"Run: VEPYR_CACHE_ROOT={root} ./run_tests <version-or-sha>",
            )
        dirs = dataset_dirs(root)
        if missing := [d for d in dirs if not d.is_dir()]:
            raise RunTestsError(
                RunTestsExit.INCOMPLETE,
                f"dataset directory {', '.join(map(str, missing))} missing "
                f"(flavour recorded in {root / fetch.PROVENANCE}). "
                f"Run: VEPYR_CACHE_ROOT={root} ./run_tests <version-or-sha>",
            )
        return (
            f"{root} (provenance and revisions match PINS.toml, FASTA present, "
            f"datasets {', '.join(d.name for d in dirs)})"
        )

    return _outcome("vepyr cache", probe, RunTestsError, OSError)


def vep_cache_check(cache_dir: Path | None) -> Check:
    """``cache_dir`` is a complete native VEP cache, as ``./bless`` requires."""
    if cache_dir is None:
        return Check("vep cache", Status.SKIP, "no --vep-cache-dir")

    def probe() -> str:
        require_complete_cache(cache_dir, flag="--vep-cache-dir")
        return f"{cache_dir} complete"

    return _outcome("vep cache", probe, BlessError, OSError)


def vep_fasta_check(fasta: Path | None) -> Check:
    """``fasta`` is an uncompressed FASTA with a ``.fai`` index next to it."""
    if fasta is None:
        return Check("vep fasta", Status.SKIP, "no --vep-fasta")

    def probe() -> str:
        require_fasta(fasta, flag="--vep-fasta")
        if not (fai := Path(f"{fasta}.fai")).is_file():
            raise BlessError(
                f"--vep-fasta {fasta} has no index {fai}; run `samtools faidx {fasta}`"
            )
        return f"{fasta} (+ .fai)"

    return _outcome("vep fasta", probe, BlessError)


def run_checks(args: argparse.Namespace, environ: Mapping[str, str]) -> list[Check]:
    """Run every check, in the documented order, and return the results.

    Args:
        args: Parsed command line.
        environ: The environment to read ``UV_PROJECT_ENVIRONMENT`` from.

    Returns:
        One result per check (nine).
    """
    return [
        uv_environment_check(environ),
        *(
            Check(f"tool {t}", Status.PASS, exe)
            if (exe := shutil.which(t))
            else Check(f"tool {t}", Status.FAIL, f"{t} not on PATH")
            for t in TOOLS
        ),
        bcftools_pin_check(),
        docker_check(args.docker_timeout),
        vepyr_cache_check(args.vepyr_cache_root),
        vep_cache_check(args.vep_cache_dir),
        vep_fasta_check(args.vep_fasta),
    ]


def _absolute(text: str) -> Path:
    """Argparse type: a user path made absolute (``~`` expanded)."""
    return Path(text).expanduser().absolute()


def _timeout(text: str) -> float:
    """Argparse type: a positive number of seconds."""
    value = float(text)
    if value <= 0:
        raise argparse.ArgumentTypeError(f"must be > 0, got {text}")
    return value


def parser() -> argparse.ArgumentParser:
    """The command-line parser."""
    p = argparse.ArgumentParser(
        prog="check_env",
        description="Check the local prerequisites of the repository tools; "
        "one PASS/FAIL/SKIP line per check.",
        epilog="Exit: 0 all checks passed (SKIP does not fail), 1 a check failed, "
        "2 UV_PROJECT_ENVIRONMENT unset/relative/inside a checkout or bad usage, "
        "3 unexpected.",
    )
    p.add_argument(
        "--vepyr-cache-root",
        type=_absolute,
        metavar="DIR",
        help="vepyr cache root (a ./run_tests cache), prechecked against PINS.toml",
    )
    p.add_argument(
        "--vep-cache-dir",
        type=_absolute,
        metavar="DIR",
        help="native VEP release-116 cache ./bless would use",
    )
    p.add_argument(
        "--vep-fasta",
        type=_absolute,
        metavar="FILE",
        help="uncompressed GRCh38 FASTA ./bless would use (needs FILE.fai)",
    )
    p.add_argument(
        "--docker-timeout",
        type=_timeout,
        default=DEFAULT_DOCKER_TIMEOUT,
        metavar="SECONDS",
        help="wall-clock limit for the docker daemon probe "
        f"(default {DEFAULT_DOCKER_TIMEOUT:g})",
    )
    return p


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point; returns the exit code (see the module docstring)."""
    args = parser().parse_args(argv)
    try:
        checks = run_checks(args, os.environ)
    except Exception as exc:  # last resort: one line, distinct exit code
        print(f"check_env: unexpected {type(exc).__name__}: {exc}", file=sys.stderr)
        return Exit.ERROR
    for c in checks:
        print(c.line())
    failed = [c.name for c in checks if c.status is Status.FAIL]
    passed = sum(c.status is Status.PASS for c in checks)
    skipped = len(checks) - passed - len(failed)
    print(
        f"{'FAIL' if failed else 'PASS'} summary: {passed} passed, "
        f"{len(failed)} failed, {skipped} skipped"
        + (f"; failed: {', '.join(failed)}" if failed else "")
    )
    if any(c.name == UV_ENV and c.status is Status.FAIL for c in checks):
        return Exit.CONFIG
    return Exit.FAIL if failed else Exit.OK


if __name__ == "__main__":
    sys.exit(main())
