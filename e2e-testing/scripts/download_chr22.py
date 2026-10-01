#!/usr/bin/env python3
"""Download and prepare chr22 data for run_comparison.py.

See the repository README for explicit Docker and comparison commands.
This helper only prepares data; run_comparison.py owns annotation and md5 checks.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys

from comparison.profiles import PROFILES

ROOT = Path(__file__).resolve().parents[2]
GOLDEN = ROOT / "e2e-testing/golden/116/chr22"
MANIFEST = GOLDEN / "manifest.json"
CORE_PROFILES = (
    "ensembl",
    "merged",
    "refseq",
    "merged_flag_pick",
    "merged_flag_pick_allele",
    "merged_flag_pick_allele_gene",
    "merged_pick_filter",
    "merged_pick_allele",
    "merged_per_gene",
    "merged_pick_allele_gene",
)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=ROOT / "e2e-testing/results/sanity-chr22",
        help="Data workspace containing input/, cache/ and output/116/",
    )
    parser.add_argument(
        "--profiles",
        nargs="+",
        choices=CORE_PROFILES,
        default=list(CORE_PROFILES),
        help="Default: all ten core profiles; use merged for a shorter check",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Use verified local files without downloads",
    )
    args = parser.parse_args(argv)
    if len(args.profiles) != len(set(args.profiles)):
        parser.error("--profiles must not contain duplicates")
    args.work_dir = args.work_dir.expanduser().resolve()
    return args


def checksum(path, algorithm="sha256"):
    digest = hashlib.new("sha1" if algorithm == "git_blob" else algorithm)
    if algorithm == "git_blob":
        digest.update(f"blob {path.stat().st_size}\0".encode())
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verified(path, metadata):
    if not path.is_file() or path.stat().st_size != metadata["size"]:
        return False
    algorithm = "sha256" if "sha256" in metadata else "git_blob"
    return checksum(path, algorithm) == metadata[algorithm]


def run(command, **kwargs):
    print("+ " + " ".join(map(str, command)), flush=True)
    return subprocess.run(list(map(str, command)), check=True, **kwargs)


def prepare_fixtures(manifest, names, offline):
    files = dict(manifest["inputs"])
    for name in names:
        reference = manifest["profiles"][name]
        files[reference["path"]] = reference
    missing = [path for path, meta in files.items() if not verified(ROOT / path, meta)]
    if missing and not offline:
        # Only these LFS objects, never the fitted test caches or other releases.
        run(
            [
                "git",
                "-c",
                f"safe.directory={ROOT}",
                "lfs",
                "pull",
                "--include=" + ",".join(files),
                "--exclude=",
            ],
            cwd=ROOT,
        )
    for path, meta in files.items():
        if not verified(ROOT / path, meta):
            raise RuntimeError(
                f"Missing or changed fixture: {path}. Restore it from this checkout's "
                "Git/LFS data; --offline requires the LFS objects to be present."
            )


def prepare_caches(manifest, names, work_dir, offline):
    for flavour in sorted({PROFILES[name].flavour for name in names}):
        spec = manifest["caches"][flavour]
        destination = work_dir / "cache" / f"116_GRCh38_{flavour}"
        missing = [
            path
            for path, meta in spec["files"].items()
            if not verified(destination / path, meta)
        ]
        if missing and not offline:
            # Explicit filenames avoid downloading any other chromosome. Force
            # only failed files, so HF's metadata cannot mask a corrupt local file.
            run(
                [
                    "hf",
                    "download",
                    spec["repo_id"],
                    *missing,
                    "--repo-type",
                    "dataset",
                    "--revision",
                    spec["revision"],
                    "--local-dir",
                    destination,
                    "--force-download",
                ]
            )
        for path, meta in spec["files"].items():
            if not verified(destination / path, meta):
                raise RuntimeError(
                    f"Missing or corrupt HF cache file: {destination / path}"
                )
        print(f"Verified {flavour} chr22 cache at {spec['revision']}", flush=True)


def prepare_workspace(manifest, names, work_dir):
    """Copy verified fixtures into the existing comparison runner's data layout."""
    inputs = work_dir / "input"
    references = work_dir / "output/116"
    inputs.mkdir(parents=True, exist_ok=True)
    references.mkdir(parents=True, exist_ok=True)
    for path, metadata in manifest["inputs"].items():
        destination = inputs / Path(path).name
        if not verified(destination, metadata):
            shutil.copyfile(ROOT / path, destination)
    for name in names:
        reference = manifest["profiles"][name]
        destination = references / f"{PROFILES[name].vep_basename}.vcf.gz"
        if not verified(destination, reference):
            shutil.copyfile(ROOT / reference["path"], destination)
        run(["tabix", "-f", "-p", "vcf", destination])


def main(argv=None):
    args = parse_args(argv)
    try:
        manifest = json.loads(MANIFEST.read_text())
        if set(manifest["profiles"]) != set(CORE_PROFILES):
            raise ValueError(
                "Golden manifest must contain exactly the ten core profiles"
            )
        if shutil.which("tabix") is None:
            raise RuntimeError("tabix is required; use the image in e2e-testing/docker")
        args.work_dir.mkdir(parents=True, exist_ok=True)
        prepare_fixtures(manifest, args.profiles, args.offline)
        prepare_caches(manifest, args.profiles, args.work_dir, args.offline)
        prepare_workspace(manifest, args.profiles, args.work_dir)
        print(f"Data ready at {args.work_dir}; run run_comparison.py to compare.")
        return 0
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
