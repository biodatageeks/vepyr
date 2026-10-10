"""Command line of ``./bless``.

Modes::

    ./bless CACHE-FLAG FASTA-FLAG <test-dir>                 # make the oracle
    ./bless --check <test-dir>                               # cheap integrity check
    ./bless --check --reproduce CACHE-FLAG FASTA-FLAG <test-dir>   # reproduction audit

The reproduction audit runs the cheap drift check first, before any docker call,
so a tampered ``expected_output.vcf`` fails it as it fails plain ``--check``.

``CACHE-FLAG`` is exactly one of ``--vep-cache-dir``/``--download-vep-cache-to-dir``
and ``FASTA-FLAG`` exactly one of ``--vep-fasta``/``--download-vep-fasta-to``.
There is no default and no environment fallback for either.

Exit codes: ``0`` success (for ``--check``: the hash matches), ``1`` any failure,
with exactly one ``bless: ...`` line on stderr naming it, ``2`` argparse usage
error (unknown flag, missing ``<test-dir>``).
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import shlex
import shutil
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import fixture_sources

from bless import BlessError, ensembl, testdir, vep

#: Default parent of the per-run Docker work directory: ``<repo-root>/.bless/``,
#: located from this file (not the caller's cwd), like ``./run_tests``'s
#: ``<repo-root>/.run_tests/``. Git-ignored.
DEFAULT_DOCKER_WORK_DIR: Final[Path] = Path(__file__).resolve().parents[2] / ".bless"

_EPILOG = """\
exactly one cache flag and one FASTA flag are required for a bless and for
--check --reproduce; plain --check needs neither, and no docker.

examples:
  ./bless --check tests/data/NAME
  ./bless --vep-cache-dir ~/vep-cache --vep-fasta ~/GRCh38.fa tests/data/NAME
  ./bless --download-vep-cache-to-dir ~/vep-cache \\
          --download-vep-fasta-to ~/GRCh38.fa tests/data/NAME
  ./bless --check --reproduce --vep-cache-dir ~/vep-cache \\
          --vep-fasta ~/GRCh38.fa tests/data/NAME
"""


@dataclass(frozen=True, slots=True, kw_only=True)
class Source:
    """A resolved cache or FASTA choice.

    Attributes:
        path: The path given.
        download: Whether ``bless`` must fetch it first.
        flag: The flag that supplied it.
    """

    path: Path
    download: bool
    flag: str


def _parser() -> argparse.ArgumentParser:
    """Build the argument parser."""
    p = argparse.ArgumentParser(
        prog="bless",
        description=(
            "Make or check a data-test's VEP 116.2 oracle (expected_output.vcf) "
            "by running "
            f"{vep.IMAGE_TAG} (pinned by digest) on its normalised input.vcf."
        ),
        epilog=_EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        allow_abbrev=False,
    )
    p.add_argument(
        "test_dir",
        type=Path,
        metavar="<test-dir>",
        help="data-test directory (holds test.toml)",
    )
    p.add_argument(
        "--check",
        action="store_true",
        help="compare the md5 of expected_output.vcf's body on disk with "
        "[compare] body_md5; "
        "no docker, cache or FASTA needed; modifies nothing",
    )
    p.add_argument(
        "--reproduce",
        action="store_true",
        help="with --check: run the drift check first, then re-run the recorded "
        "VEP image into a temp dir and compare that "
        "fresh body md5 with the stored one (needs docker, cache and FASTA)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="print the exact command(s) and run nothing",
    )
    p.add_argument(
        "--docker-work-dir",
        type=Path,
        metavar="PATH",
        default=None,
        help="directory under which each run gets its own bless-* work "
        "directory, bind-mounted into the VEP container; must be shared with "
        f"Docker (default: <repo-root>/.bless/, here {DEFAULT_DOCKER_WORK_DIR})",
    )
    p.add_argument(
        "--vep-flag",
        action="append",
        default=[],
        metavar="FLAG",
        help="extra VEP flag, appended after the fixed command and recorded in "
        "[vep] extra_flags (without it, a recorded extra_flags is used; a "
        "different one is refused); repeatable; use the = form "
        "(--vep-flag=--check_existing); "
        f"allowed: {', '.join(vep.ALLOWED_VEP_FLAGS)}; refused with --check",
    )
    cache = p.add_argument_group("VEP cache (exactly one, no default)")
    cache.add_argument(
        "--vep-cache-dir",
        type=Path,
        metavar="PATH",
        help="existing complete release-116 cache",
    )
    cache.add_argument(
        "--download-vep-cache-to-dir",
        type=Path,
        metavar="PATH",
        help="fetch the Ensembl release-116 indexed cache into PATH "
        "(checksum-verified), then use it",
    )
    fasta = p.add_argument_group("GRCh38 FASTA (exactly one, no default)")
    fasta.add_argument(
        "--vep-fasta",
        type=Path,
        metavar="PATH",
        help="existing uncompressed GRCh38 FASTA",
    )
    fasta.add_argument(
        "--download-vep-fasta-to",
        type=Path,
        metavar="PATH",
        help="fetch Ensembl's GRCh38 primary-assembly FASTA (checksum-verified) "
        "to PATH, then use it",
    )
    return p


def _pick(
    existing: Path | None, download: Path | None, *, have: str, fetch: str, what: str
) -> Source:
    """Apply the exactly-one-of rule to a flag pair.

    Raises:
        BlessError: If neither or both flags were given.
    """
    match existing, download:
        case None, None:
            raise BlessError(
                f"no {what} given: pass exactly one of {have} PATH or {fetch} PATH "
                "(there is no default)"
            )
        case Path(), Path():
            raise BlessError(
                f"{have} and {fetch} are mutually exclusive: pass exactly one of them"
            )
        case Path() as path, None:
            return Source(path=path.expanduser().absolute(), download=False, flag=have)
        case None, Path() as path:
            return Source(path=path.expanduser().absolute(), download=True, flag=fetch)
    raise AssertionError("unreachable")


def _print_check_plan(test: testdir.TestDir) -> None:
    """Print what :func:`_check` would compare (the ``--dry-run`` form)."""
    print(
        f"# md5 of non-# lines of {test.oracle} vs [compare] body_md5 "
        f"in {test.toml_path}"
    )


def _check(test: testdir.TestDir) -> None:
    """The cheap integrity check.

    Raises:
        BlessError: On mismatch or missing pieces.
    """
    stored = test.stored_md5()
    if not test.oracle.is_file():
        raise BlessError(
            f"{test.oracle} does not exist; bless the directory with "
            "`./bless <test-dir>`"
        )
    found = testdir.body_md5(test.oracle)
    if found != stored:
        raise BlessError(
            f"{test.oracle} body md5 {found} != [compare] body_md5 {stored} "
            f"in {test.toml_path}: "
            "the file drifted from its recorded hash"
        )
    print(f"bless: {test.root}: body md5 {found} matches test.toml")


@dataclass(frozen=True, slots=True, kw_only=True)
class VepRun:
    """Result of one VEP run.

    Attributes:
        work: Temp directory holding ``expected_output.vcf`` (caller removes it).
        image: The digest-pinned image that ran.
    """

    work: Path
    image: str


def _run_vep(
    test: testdir.TestDir,
    cache: Source,
    fasta: Source,
    *,
    image: str | None,
    extra: tuple[str, ...],
    work_root: Path,
    dry_run: bool,
) -> VepRun | None:
    """Validate sources, fetch if asked, and run VEP in a fresh temp dir.

    Args:
        test: The data-test.
        cache: Cache choice.
        fasta: FASTA choice.
        image: Pinned image to run, or ``None`` to resolve :data:`vep.IMAGE_TAG`.
        extra: Validated extra VEP flags.
        work_root: Parent of the per-run work directory (created if missing).
        dry_run: Print the command instead of running it.

    Returns:
        The run, or ``None`` for a dry run.

    Raises:
        BlessError: On any failure.
    """
    vep.require_cache_mode(test.config, extra, where=str(test.toml_path))
    merged = "--merged" in extra
    if merged and cache.download:
        raise BlessError("merged cache download is not supported; use --vep-cache-dir")
    if dry_run:
        for src, art in ((cache, ensembl.CACHE), (fasta, ensembl.FASTA)):
            if src.download:
                print(
                    f"# fetch {art.url} (verify Ensembl sum {art.ensembl_sum!r}) "
                    f"-> {src.path}"
                )
        work = work_root / "bless-XXXXXX"
        print(f"# cp {test.input_vcf} {work}/{testdir.INPUT_NAME}")
        mounts = vep.Mounts(cache_dir=cache.path, fasta=fasta.path, work_dir=work)
        print(shlex.join(vep.docker_argv(image or vep.IMAGE_TAG, mounts, extra)))
        return None
    if not cache.download:
        ensembl.require_complete_cache(cache.path, flag=cache.flag, merged=merged)
    if not fasta.download:
        ensembl.require_fasta(fasta.path, flag=fasta.flag)
    docker = vep.require_docker()
    for directory, flag in ((cache.path, cache.flag), (fasta.path.parent, fasta.flag)):
        try:
            directory.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise BlessError(
                f"cannot create {directory} for {flag}: {exc.strerror or exc}"
            ) from exc
    pinned = vep.resolve_digest(docker, image or vep.IMAGE_TAG)
    try:
        work_root.mkdir(parents=True, exist_ok=True)
        work = Path(tempfile.mkdtemp(prefix="bless-", dir=work_root))
    except OSError as exc:
        raise BlessError(
            f"cannot create a work directory in {work_root} for --docker-work-dir: "
            f"{exc.strerror or exc}"
        ) from exc
    try:
        # Probe before any download: a path Docker cannot mount fails in seconds.
        vep.require_mountable(docker, pinned, [cache.path, fasta.path.parent, work])
        if cache.download:
            ensembl.download_cache(cache.path)
        if fasta.download:
            ensembl.download_fasta(fasta.path)
        ensembl.ensure_fai(fasta.path)
        shutil.copyfile(test.input_vcf, work / testdir.INPUT_NAME)
        input_sha = hashlib.sha256(test.input_vcf.read_bytes()).hexdigest()
        if (
            hashlib.sha256((work / testdir.INPUT_NAME).read_bytes()).hexdigest()
            != input_sha
        ):
            raise BlessError("Docker input copy differs from the normalized input.vcf")
        print(f"bless: Docker input SHA256 {input_sha}", file=sys.stderr, flush=True)
        print(
            f"bless: running {pinned} on {test.input_vcf}", file=sys.stderr, flush=True
        )
        vep.run(
            docker,
            pinned,
            vep.Mounts(cache_dir=cache.path, fasta=fasta.path, work_dir=work),
            extra,
        )
        if any(
            hashlib.sha256(path.read_bytes()).hexdigest() != input_sha
            for path in (test.input_vcf, work / testdir.INPUT_NAME)
        ):
            raise BlessError("normalized input.vcf changed during the VEP run")
    except BaseException:
        shutil.rmtree(work, ignore_errors=True)
        raise
    return VepRun(work=work, image=pinned)


def _bless(
    test: testdir.TestDir,
    cache: Source,
    fasta: Source,
    *,
    extra: tuple[str, ...],
    work_root: Path,
    dry_run: bool,
) -> None:
    """Make the oracle and record its metadata, including the extra flags.

    The flags are ``extra`` (from ``--vep-flag=``) or, when none is typed, the
    ``[vep] extra_flags`` already in ``test.toml``; typed flags that differ
    from a recorded list are refused. A non-empty list is written back as
    ``[vep] extra_flags``, and ``[vep] command`` is generated from it.

    Raises:
        BlessError: On any failure; the directory is then left unchanged.
    """
    where = str(test.toml_path)
    vep.require_vepyr_mode(test.config, where=where)
    listed = vep.recorded_flags(test.table("vep"), where=where)
    if extra and listed and extra != listed:
        raise BlessError(
            f"{where}: --vep-flag= gives {list(extra)!r} but [vep] extra_flags is "
            f"{list(listed)!r}; edit extra_flags in test.toml to change the flags"
        )
    extra = extra or listed
    run = _run_vep(
        test,
        cache,
        fasta,
        image=None,
        extra=extra,
        work_root=work_root,
        dry_run=dry_run,
    )
    if run is None:
        return
    work, pinned = run.work, run.image
    try:
        fresh = work / testdir.ORACLE_NAME
        md5 = testdir.body_md5(fresh)
        cprov, fprov = (
            ensembl.cache_provenance(cache.path, merged="--merged" in extra),
            ensembl.fasta_provenance(fasta.path),
        )
        flags: dict[str, str | list[str]] = (
            {"extra_flags": list(extra)} if extra else {}
        )
        updated = testdir.set_keys(
            test.toml_path.read_text(encoding="utf-8"),
            {
                "vep": {
                    **flags,
                    "image": pinned,
                    "command": vep.vep_command(extra),
                    "date": dt.datetime.now(dt.UTC).date().isoformat(),
                    "cache_source": fixture_sources.cache_source(
                        test.table("vepyr").get("flavour", "ensembl")
                    ),
                    "vep_cache": cprov.source,
                    "vep_cache_checksum": cprov.checksum,
                    "fasta_source": fprov.source,
                    "fasta_checksum": fprov.checksum,
                },
                "compare": {"body_md5": md5},
            },
        )
        testdir.write_atomically(test.oracle, fresh.read_bytes())
        testdir.write_atomically(test.toml_path, updated)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    print(f"bless: wrote {test.oracle} (body md5 {md5}, image {pinned})")


def _reproduce(
    test: testdir.TestDir,
    cache: Source,
    fasta: Source,
    *,
    extra: tuple[str, ...],
    work_root: Path,
    dry_run: bool,
) -> None:
    """Re-run the recorded image and command and compare the fresh body md5.

    The drift check (:func:`_check`) runs first, before the image validation and
    any docker call, so the on-disk oracle must match ``[compare] body_md5``; the
    fresh-vs-stored comparison then also proves fresh output == file on disk.
    On ``dry_run`` the drift check is skipped and only described.

    ``extra`` is always empty (typed flags are refused under ``--check``); the
    flags come from ``[vep] extra_flags``, checked against the allowlist, and
    ``[vep] command`` must be exactly the command generated from them.

    Raises:
        BlessError: On mismatch or any failure.
    """
    if dry_run:
        _print_check_plan(test)
    else:
        _check(test)
    stored = test.stored_md5()
    image = test.table("vep").get("image")
    if not isinstance(image, str) or not image.startswith(f"{vep.IMAGE_REPO}@sha256:"):
        raise BlessError(
            f"{test.toml_path}: [vep] image is missing or not pinned by digest "
            f"({vep.IMAGE_REPO}@sha256:...); bless the directory first"
        )
    where = str(test.toml_path)
    vep.require_vepyr_mode(test.config, where=where)
    recorded = vep.recorded_flags(test.table("vep"), where=where)
    vep.require_command(test.table("vep").get("command"), recorded, where=where)
    run = _run_vep(
        test,
        cache,
        fasta,
        image=image,
        extra=recorded,
        work_root=work_root,
        dry_run=dry_run,
    )
    if run is None:
        return
    work = run.work
    try:
        fresh = testdir.body_md5(work / testdir.ORACLE_NAME)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    if fresh != stored:
        raise BlessError(
            f"reproduction failed: re-running {image} gives body md5 {fresh}, "
            f"[compare] body_md5 in {test.toml_path} is {stored}"
        )
    print(f"bless: {test.root}: re-running {image} reproduces body md5 {fresh}")


def main(argv: list[str] | None = None) -> int:
    """Entry point.

    Args:
        argv: Arguments; ``None`` means :data:`sys.argv`.

    Returns:
        Exit code.
    """
    parser = _parser()
    raw = sys.argv[1:] if argv is None else argv
    if "--vep-flag" in raw[: raw.index("--") if "--" in raw else len(raw)]:
        parser.error(
            "--vep-flag needs the = form, because a VEP flag starts with '-': "
            "write --vep-flag=<flag> (e.g. --vep-flag=--check_existing)"
        )
    args = parser.parse_args(argv)
    try:
        if args.reproduce and not args.check:
            raise BlessError(
                "--reproduce only works together with --check "
                "(./bless --check --reproduce ...)"
            )
        if args.check and args.vep_flag:
            raise BlessError(
                "--vep-flag is refused with --check: a check replays only what "
                "[vep] extra_flags records"
            )
        extra = vep.parse_extra(args.vep_flag)
        cheap_check = args.check and not args.reproduce
        if cheap_check:
            test = testdir.load(args.test_dir)
            if args.dry_run:
                _print_check_plan(test)
                return 0
            _check(test)
            return 0
        cache = _pick(
            args.vep_cache_dir,
            args.download_vep_cache_to_dir,
            have="--vep-cache-dir",
            fetch="--download-vep-cache-to-dir",
            what="VEP cache",
        )
        fasta = _pick(
            args.vep_fasta,
            args.download_vep_fasta_to,
            have="--vep-fasta",
            fetch="--download-vep-fasta-to",
            what="FASTA",
        )
        test = testdir.load(args.test_dir)
        test.require_normalised_input()
        work_root = (
            DEFAULT_DOCKER_WORK_DIR
            if args.docker_work_dir is None
            else args.docker_work_dir.expanduser().absolute()
        )
        run = _reproduce if args.reproduce else _bless
        run(test, cache, fasta, extra=extra, work_root=work_root, dry_run=args.dry_run)
    except BlessError as exc:
        print(f"bless: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("bless: interrupted; downloads resume on the next run", file=sys.stderr)
        return 130
    return 0
