"""One argument: a vepyr release version or full Git SHA. Run only data tests."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from run_tests import fetch, fixtures, install, suite, tests
from run_tests.progress import Progress
from run_tests.verdict import Exit, RunTestsError


def parse_args(argv):
    parser = argparse.ArgumentParser(
        prog="./run_tests",
        allow_abbrev=False,
        description="Run data tests with a vepyr PyPI wheel or a Git build using uv.",
    )
    parser.add_argument(
        "vepyr",
        metavar="VERSION_OR_SHA",
        help="PyPI release (e.g. 0.9.0) or a full 40-character Git commit SHA",
    )
    return install.validate_target(parser.parse_args(argv).vepyr)


def cache_root() -> Path:
    return (
        Path(os.environ.get("VEPYR_CACHE_ROOT") or Path.home() / "vepyr-test-cache")
        .expanduser()
        .resolve()
    )


def prepare_cache(cases: list[fixtures.Fixture], root: Path, repo: Path) -> Path:
    pins_path = repo / "PINS.toml"
    pins, fasta_pin = fetch.load_dataset_pins(pins_path)
    if fasta_pin is None:
        raise RunTestsError(Exit.USAGE, "PINS.toml lacks the reference FASTA pin")
    contigs = sorted(
        {
            c
            for case in cases
            if not case.skip
            for run in case.runs
            for c in run["required_contigs"]
        }
    )
    print(
        f"Merged cache {pins[fetch.Flavour.MERGED].revision}; "
        f"contigs: {', '.join(contigs)}",
        flush=True,
    )
    fetch.fetch(
        fetch.Selection(
            root=root,
            flavours=(fetch.Flavour.MERGED,),
            contigs=tuple(contigs),
            fasta=True,
            trim_manifests=True,
            verify=False,
            dry_run=False,
        ),
        pins,
        fasta_pin,
        lister=fetch.hub_lister,
        downloader=fetch.hub_downloader,
        fasta_fetcher=fetch.url_fetcher,
        pins_toml=pins_path,
        argv=sys.argv[1:],
    )
    tests.precheck_cache(root, pins_toml=pins_path, flavours=("merged",))
    for entity in fetch.ENTITIES:
        entity_dir = root / fetch.Flavour.MERGED.dir_name / entity
        try:
            entries = json.loads((entity_dir / fetch.MANIFEST).read_text())
            listed = {entry["dataset"] for entry in entries}
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise RunTestsError(
                Exit.INCOMPLETE, f"unusable {entity_dir / fetch.MANIFEST}: {exc}"
            ) from exc
        for contig in contigs:
            if contig == "chrMT" and entity in {"motif", "regulatory"}:
                continue
            path = entity_dir / f"{contig}.parquet"
            if not path.is_file() or path.name not in listed:
                raise RunTestsError(
                    Exit.INCOMPLETE,
                    f"required cache shard missing or unlisted in manifest: {path}",
                )
    return root / fetch.FASTA_DIR / fasta_pin.fa_name


def run_selection(
    target: str,
    directories,
    *,
    root: Path | None = None,
    repo: Path | None = None,
    installer=None,
    preparer=None,
    runner=None,
) -> int:
    """Programmatic selection for campaign tooling; the public CLI runs all tests."""
    target = install.validate_target(target)
    repo = fixtures.ROOT if repo is None else repo
    root = cache_root() if root is None else root.resolve()
    progress = Progress("Preparing data tests", 3, unit="steps")
    cases = fixtures.load_all(directories)
    progress.update(1, force=True)
    total = sum(len(case.ids) for case in cases)
    print(f"{total} named tests in {len(cases)} fixtures; cache: {root}", flush=True)
    if all(case.skip for case in cases):
        build = install.CliBuild(
            Path(sys.executable), target, "not installed (all skipped)"
        )
        fasta = Path("unused")
    else:
        fasta = (preparer or prepare_cache)(cases, root, repo)
        progress.update(2, force=True)
        build = (installer or install.install)(target, root)
    progress.update(3, force=True)
    print(build.label, flush=True)
    report = suite.run(cases, build=build, cache_root=root, fasta=fasta, runner=runner)
    print(f"run_tests: {report.detail}; exit {int(report.code)}", flush=True)
    return int(report.code)


def main(argv=None) -> int:
    try:
        target = parse_args(sys.argv[1:] if argv is None else argv)
        repo = fixtures.ROOT
        directories = sorted(
            path for path in (repo / tests.DATA_DIR).iterdir() if path.is_dir()
        )
        return run_selection(target, directories)
    except RunTestsError as exc:
        print(
            f"run_tests: error ({exc.code.name.lower()}, exit {int(exc.code)}): {exc}",
            file=sys.stderr,
            flush=True,
        )
        return int(exc.code)
    except OSError as exc:
        print(f"run_tests: {exc}", file=sys.stderr, flush=True)
        return int(Exit.INCOMPLETE)
