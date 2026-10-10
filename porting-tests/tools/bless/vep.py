"""The pinned VEP docker image and the one fixed VEP command.

The command is the one from the #16 reference comment (5671600272). Inside the
container every path is fixed — cache at :data:`CACHE_MOUNT`, FASTA at
:data:`FASTA_MOUNT`, input and output in the working directory — so the
recorded ``[vep] command`` is the same string on every host. Extra flags are
recorded as data in ``[vep] extra_flags``; the command is generated from them.

There is one data-test mode, VEP ``--everything`` (#143): :data:`MAPPING_FILE`
pairs each flag of the fixed command with the ``[vepyr]`` value of
``test.toml`` that reproduces it in vepyr, and :func:`require_vepyr_mode`
refuses a ``test.toml`` that disagrees.
"""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from bless import BlessError
from bless.testdir import INPUT_NAME, ORACLE_NAME
from vep_pin import load_pin

IMAGE_TAG: Final[str] = load_pin().image_tag
"""Ensembl's official image, from ``tools/vep_pin.toml`` (#239); ``bless``
resolves and records its digest."""

IMAGE_REPO: Final[str] = load_pin().image_repo
CACHE_MOUNT: Final[str] = "/opt/vep/.vep"
FASTA_MOUNT: Final[str] = "/opt/vep/fasta/Homo_sapiens.GRCh38.dna.primary_assembly.fa"
WORK_MOUNT: Final[str] = "/work"

VEP_ARGV: Final[tuple[str, ...]] = (
    "vep",
    "--offline",
    "--cache",
    "--dir_cache", CACHE_MOUNT,
    "--species", "homo_sapiens",
    "--cache_version", load_pin().cache_version,
    "--assembly", "GRCh38",
    "--fasta", FASTA_MOUNT,
    "--everything",
    "--vcf",
    "--input_file", INPUT_NAME,
    "--output_file", ORACLE_NAME,
    "--force_overwrite",
)  # fmt: skip

MAPPING_FILE: Final[Path] = Path(__file__).resolve().parents[1] / "vep_flags.toml"
"""``tools/vep_flags.toml``: the VEP flag -> ``[vepyr]`` mapping (#143), also
read by ``tools/run_tests/fixtures.py``."""


@dataclass(frozen=True, slots=True, kw_only=True)
class ModeRow:
    """One ``[[mapping]]`` row of :data:`MAPPING_FILE`.

    Attributes:
        vep_flag: A flag of :data:`VEP_ARGV`.
        vepyr_key: The ``[vepyr]`` key of ``test.toml`` it maps onto.
        vepyr_value: The value that key must hold.
        note: Why the two correspond (shown in the README table).
    """

    vep_flag: str
    vepyr_key: str
    vepyr_value: bool
    note: str


def mode_mapping(path: Path = MAPPING_FILE) -> tuple[ModeRow, ...]:
    """Read the VEP flag -> ``[vepyr]`` mapping.

    Args:
        path: The mapping file (default :data:`MAPPING_FILE`).

    Returns:
        The rows in file order.

    Raises:
        BlessError: If the file is unreadable, empty or a row is malformed.
    """
    try:
        rows = tomllib.loads(path.read_text(encoding="utf-8")).get("mapping")
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise BlessError(f"cannot read the mode mapping {path}: {exc}") from exc
    if not isinstance(rows, list) or not rows:
        raise BlessError(f"{path}: [[mapping]] must list at least one row")
    parsed: list[ModeRow] = []
    for row in rows:
        match row:
            case {
                "vep_flag": str(flag),
                "vepyr_key": str(key),
                "vepyr_value": bool(value),
                "note": str(note),
            } if len(row) == 4:
                parsed.append(
                    ModeRow(vep_flag=flag, vepyr_key=key, vepyr_value=value, note=note)
                )
            case _:
                raise BlessError(f"{path}: malformed [[mapping]] row {row!r}")
    return tuple(parsed)


def require_vepyr_mode(config: Mapping[str, object], *, where: str) -> None:
    """Require ``test.toml`` to be in the one data-test mode (``--everything``).

    Every ``[vepyr]`` value named by :func:`mode_mapping` must hold, in
    ``[vepyr]`` and in each ``[[vepyr_run]]`` that overrides it.

    Args:
        config: The parsed ``test.toml``.
        where: File named in error messages.

    Raises:
        BlessError: On a missing ``[vepyr]`` table or a value that differs.
    """
    match config.get("vepyr"):
        case dict() as base:
            pass
        case _:
            raise BlessError(f"{where}: [vepyr] is missing")
    match config.get("vepyr_run", []):
        case list() as runs:
            overrides = [run for run in runs if isinstance(run, dict)]
        case _:
            overrides = []
    for row in mode_mapping():
        for table in (base, *overrides):
            found = table.get(row.vepyr_key, base.get(row.vepyr_key))
            if found is not row.vepyr_value:
                shown = str(found).lower() if isinstance(found, bool) else repr(found)
                raise BlessError(
                    f"{where}: unsupported mode: [vepyr] {row.vepyr_key} = "
                    f"{shown}; the only data-test mode is VEP --everything, where "
                    f"VEP {row.vep_flag} maps to {row.vepyr_key} = "
                    f"{str(row.vepyr_value).lower()} ({MAPPING_FILE.name})"
                )


ALLOWED_VEP_FLAGS: Final[tuple[str, ...]] = (
    "--check_existing",  # needed by #18
    "--merged",  # cache selection; must agree with every vepyr run
)
"""The only flags ``--vep-flag=`` accepts, matched exactly on the whole token.

VEP's Getopt::Long accepts aliases and unique-prefix abbreviations, so a
denylist could never be complete; an exact allowlist cannot be bypassed.
Adding a flag is one line here plus a data-test that needs it. Boolean flags
only: a flag with a value needs its own design.
"""


def parse_extra(values: list[str] | tuple[str, ...]) -> tuple[str, ...]:
    """Validate extra VEP flags against :data:`ALLOWED_VEP_FLAGS`.

    Args:
        values: Flags in the order given (``--vep-flag=`` values or
            ``[vep] extra_flags``).

    Returns:
        The flags, unchanged and in order.

    Raises:
        BlessError: On a flag not in the allowlist (exact match) or a duplicate.
    """
    allowed = ", ".join(ALLOWED_VEP_FLAGS)
    seen: set[str] = set()
    for flag in values:
        if flag not in ALLOWED_VEP_FLAGS:
            raise BlessError(f"--vep-flag: {flag} is not allowed; allowed: {allowed}")
        if flag in seen:
            raise BlessError(f"--vep-flag: {flag} is given twice")
        seen.add(flag)
    return tuple(values)


def require_cache_mode(
    config: Mapping[str, object], extra: tuple[str, ...], *, where: str
) -> None:
    """Prevent comparison of an Ensembl oracle against a merged engine cache."""
    base = config.get("vepyr", {})
    if not isinstance(base, dict):
        raise BlessError(f"{where}: [vepyr] is missing")
    expected = "merged" if "--merged" in extra else "ensembl"
    runs = config.get("vepyr_run", [])
    if not isinstance(runs, list):
        raise BlessError(f"{where}: vepyr_run must be an array")
    for run in [base, *runs]:
        if not isinstance(run, dict):
            raise BlessError(f"{where}: invalid vepyr run")
        found = run.get("flavour", base.get("flavour"))
        if found != expected:
            raise BlessError(
                f"{where}: cache flavour mismatch: VEP uses {expected}, "
                f"vepyr uses {found}"
            )


def vep_command(extra: tuple[str, ...] = ()) -> str:
    """Return the ``[vep] command`` string for the fixed argv plus ``extra``."""
    return shlex.join((*VEP_ARGV, *extra))


def recorded_flags(table: Mapping[str, object], *, where: str) -> tuple[str, ...]:
    """Read and validate ``[vep] extra_flags`` (absent means no extra flags).

    Args:
        table: The ``[vep]`` table of ``test.toml``.
        where: File named in error messages.

    Returns:
        The flags in their recorded order, checked by :func:`parse_extra`.

    Raises:
        BlessError: If the key is not an array of strings or a flag is not
            allowed or repeated.
    """
    match table.get("extra_flags", []):
        case list() as flags if all(isinstance(flag, str) for flag in flags):
            pass
        case other:
            raise BlessError(
                f"{where}: [vep] extra_flags must be an array of strings, got {other!r}"
            )
    try:
        return parse_extra(flags)
    except BlessError as exc:
        raise BlessError(f"{where}: [vep] extra_flags: {exc}") from exc


def require_command(command: object, extra: tuple[str, ...], *, where: str) -> None:
    """Require the recorded ``[vep] command`` to be generated from ``extra``.

    The command is an audit record, never parsed: it must equal
    :func:`vep_command` of ``[vep] extra_flags`` exactly (canonical form).

    Args:
        command: The recorded ``[vep] command`` value.
        extra: The validated ``[vep] extra_flags``.
        where: File named in error messages.

    Raises:
        BlessError: If the command is missing, not a string, or differs.
    """
    if not isinstance(command, str):
        raise BlessError(f"{where}: [vep] command is missing")
    if command != (canonical := vep_command(extra)):
        raise BlessError(
            f"{where}: [vep] command is not the canonical command for [vep] "
            f"extra_flags = {list(extra)!r}; recorded: {command!r}; "
            f"expected exactly: {canonical!r}"
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class Mounts:
    """Host paths bound into the container.

    Attributes:
        cache_dir: VEP ``--dir_cache`` root (read-only).
        fasta: Uncompressed FASTA (read-only; its ``.fai`` next to it).
        work_dir: Directory holding ``input.vcf``; VEP writes its output here.
    """

    cache_dir: Path
    fasta: Path
    work_dir: Path


def docker_argv(image: str, mounts: Mounts, extra: tuple[str, ...] = ()) -> list[str]:
    """Build the full ``docker run`` argv.

    Args:
        image: Image reference (tag for a dry run, digest for a real one).
        mounts: Host paths.
        extra: Validated extra VEP flags, appended after :data:`VEP_ARGV`.

    Returns:
        The argv, ready for :func:`subprocess.run` or :func:`shlex.join`.
    """
    fai = mounts.fasta.with_name(mounts.fasta.name + ".fai")
    return [
        "docker", "run", "--rm",
        "--user", f"{os.getuid()}:{os.getgid()}",
        "-v", f"{mounts.cache_dir}:{CACHE_MOUNT}:ro",
        "-v", f"{mounts.fasta}:{FASTA_MOUNT}:ro",
        "-v", f"{fai}:{FASTA_MOUNT}.fai:ro",
        "-v", f"{mounts.work_dir}:{WORK_MOUNT}",
        "-w", WORK_MOUNT,
        image,
        *VEP_ARGV,
        *extra,
    ]  # fmt: skip


def require_docker(timeout: float | None = None) -> str:
    """Return the docker executable, or fail with a readable message.

    Args:
        timeout: Wall-clock limit in seconds for the daemon probe; ``None``
            (the default, what ``./bless`` uses) waits indefinitely.

    Raises:
        BlessError: If docker is missing, its daemon does not answer, or the
            probe exceeds ``timeout``.
    """
    exe = shutil.which("docker")
    if exe is None:
        raise BlessError(
            "docker is not on PATH; install Docker (or run "
            "`./bless --check <test-dir>`, "
            "which needs no docker)"
        )
    try:
        probe = subprocess.run(
            [exe, "info", "--format", "{{.ServerVersion}}"],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise BlessError(
            "docker daemon did not answer: `docker info` timed out after "
            f"{exc.timeout:g}s"
        ) from exc
    # docker 27 exits 0 with an empty stdout when the daemon is unreachable
    # (the error goes to stderr only), so an empty server version is a failure.
    if probe.returncode != 0 or not probe.stdout.strip():
        raise BlessError(
            "docker is installed but its daemon does not answer: "
            f"{probe.stderr.strip()[:200] or 'empty server version'}"
        )
    return exe


def resolve_digest(docker: str, reference: str) -> str:
    """Pull ``reference`` and return it pinned as ``repo@sha256:...``.

    Args:
        docker: Docker executable.
        reference: Tag or digest reference to pull.

    Raises:
        BlessError: If the pull fails or the image has no registry digest.
    """
    pull = subprocess.run(
        [docker, "pull", "--quiet", reference], capture_output=True, text=True
    )
    if pull.returncode != 0:
        raise BlessError(f"docker pull {reference} failed: {pull.stderr.strip()[:300]}")
    inspect = subprocess.run(
        [docker, "image", "inspect", "--format", "{{json .RepoDigests}}", reference],
        capture_output=True,
        text=True,
    )
    digests = [
        d for d in inspect.stdout.strip().strip("[]").replace('"', "").split(",") if d
    ]
    pinned = next((d for d in digests if d.startswith(f"{IMAGE_REPO}@sha256:")), None)
    if inspect.returncode != 0 or pinned is None:
        raise BlessError(
            f"could not read the registry digest of {reference} "
            "from `docker image inspect`"
        )
    return pinned


def require_mountable(docker: str, image: str, paths: list[Path]) -> None:
    """Fail early unless docker can bind-mount every path in ``paths``.

    Docker Desktop only shares the host directories listed in its settings.
    A path outside them either fails when a container starts ("Mounts
    denied") or, for paths that also exist inside Docker's VM such as
    ``/var/folders``, is silently mounted as an *empty* VM directory. So the
    probe lists each directory from inside a container and compares that with
    the host's listing; an empty directory gets a short-lived marker file so
    the comparison means something. Probing first means a non-shared
    ``--download-vep-cache-to-dir`` is refused before a 27 GB download, not
    after it.

    Args:
        docker: Docker executable.
        image: An image already present locally.
        paths: Existing host directories.

    Raises:
        BlessError: Naming the first path the container cannot see.
    """
    fix = (
        "share it in Docker Desktop (Settings > Resources > File sharing) or pick "
        "a path inside a shared directory"
    )
    for path in paths:
        marker: Path | None = None
        if not any(path.iterdir()):
            marker = path / ".bless-mount-probe"
            marker.touch()
        try:
            host = {p.name for p in path.iterdir()}
            argv = [docker, "run", "--rm", "--entrypoint", "ls"]
            argv += ["-v", f"{path}:/probe:ro", image, "-1A", "/probe"]
            probe = subprocess.run(
                argv,
                capture_output=True,
                text=True,
            )
        finally:
            if marker is not None:
                marker.unlink(missing_ok=True)
        if probe.returncode != 0:
            reason = next(
                (ln for ln in probe.stderr.splitlines() if "not shared" in ln),
                probe.stderr.strip().splitlines()[-1] if probe.stderr.strip() else "",
            )
            raise BlessError(f"docker cannot mount {path} ({reason.strip()}); {fix}")
        if not host <= set(probe.stdout.splitlines()):
            raise BlessError(
                f"docker mounts {path} but the container does not see its files "
                f"(Docker Desktop does not share that path); {fix}"
            )


def run(docker: str, image: str, mounts: Mounts, extra: tuple[str, ...] = ()) -> None:
    """Run VEP with ``extra`` flags; its own output streams to the terminal.

    Raises:
        BlessError: If the container exits non-zero or writes no output.
    """
    argv = docker_argv(image, mounts, extra)
    argv[0] = docker
    done = subprocess.run(argv)
    if done.returncode != 0:
        raise BlessError(
            f"VEP in {image} exited with {done.returncode}; its own messages are above"
        )
    if not (mounts.work_dir / ORACLE_NAME).is_file():
        raise BlessError(f"VEP in {image} exited 0 but wrote no {ORACLE_NAME}")
