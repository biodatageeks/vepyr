#!/usr/bin/env python3
"""Download pinned HF chr22 caches and check ten profiles against VEP 116.

See the repository README for the Docker command. No Ensembl VEP installation
is needed. Each profile runs through run_comparison.py in a fresh process.
"""

from __future__ import annotations

import argparse
import csv
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
        default=ROOT / "e2e-testing/results/reviewer-chr22",
        help="Persistent downloads, logs, outputs and summary.tsv",
    )
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument(
        "--profiles",
        nargs="+",
        choices=CORE_PROFILES,
        default=list(CORE_PROFILES),
        help="Default: all ten core profiles; use merged for a shorter check",
    )
    parser.add_argument(
        "--prepare-only", action="store_true", help="Fetch and verify inputs, then stop"
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Use verified local files without downloads",
    )
    args = parser.parse_args(argv)
    if args.workers < 1:
        parser.error("--workers must be positive")
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


def profile_result(name, report_path, returncode, expected):
    """Require fresh, complete, nonempty evidence; a subprocess exit alone isn't a pass."""
    row = {"profile": name, "status": "ERROR", "exit_code": returncode}
    try:
        report = json.loads(report_path.read_text())
        if (report["profile"], report["release"], report["chrom"]) != (
            name,
            "116",
            "chr22",
        ):
            return row
        md5 = report["md5"]
        ok = returncode == 0
        for mode in ("strict", "canonical"):
            entry = md5[mode]
            row[f"vep_{mode}_md5"] = entry["vep_body_md5"]
            row[f"vepyr_{mode}_md5"] = entry["vepyr_body_md5"]
            ok &= (
                entry["body_match"]
                and entry["count_match"]
                and entry["vep_records"]
                == entry["vepyr_records"]
                == expected["records"]
                > 0
                and entry["vep_body_md5"]
                == entry["vepyr_body_md5"]
                == expected[f"{mode}_body_md5"]
                and entry["csq_order_ignored"] == PROFILES[name].ignore_csq_order
            )
        row["records"] = md5["strict"]["vepyr_records"]
        row["csq_order_ignored"] = PROFILES[name].ignore_csq_order
        row["status"] = "PASS" if ok else "FAIL"
    except (OSError, ValueError, KeyError, TypeError):
        pass
    return row


def compare_profiles(args, manifest):
    work = args.work_dir
    logs = work / "logs"
    references = work / "references"
    reports = work / "reports"
    for directory in (logs, references, reports):
        directory.mkdir(parents=True, exist_ok=True)
    inputs = work / "input"
    inputs.mkdir(parents=True, exist_ok=True)
    for path, metadata in manifest["inputs"].items():
        destination = inputs / Path(path).name
        if not verified(destination, metadata):
            shutil.copyfile(ROOT / path, destination)
    rows = []
    summary = work / "summary.tsv"
    # Invalidate a previous green summary before any new subprocess can fail.
    summary.write_text("")
    for index, name in enumerate(args.profiles, 1):
        expected = manifest["profiles"][name]
        reference = references / f"{name}.vcf.gz"
        if not verified(reference, expected):
            shutil.copyfile(ROOT / expected["path"], reference)
        run(["tabix", "-f", "-p", "vcf", reference])
        report_path = reports / f"fast_chr22_{PROFILES[name].suffix}_116_report.json"
        report_path.unlink(missing_ok=True)
        command = [
            sys.executable,
            ROOT / "e2e-testing/scripts/run_comparison.py",
            "--release",
            "116",
            "--profile",
            name,
            "--chroms",
            "22",
            "--comparison-mode",
            "md5",
            "--md5-mode",
            "both",
            "--bgzf",
            "--workers",
            str(args.workers),
            "--no-normalize",
            "--vcf",
            inputs / Path(manifest["input_vcf"]).name,
            "--fasta",
            inputs / Path(manifest["fasta"]).name,
            "--vep",
            reference,
            "--cache-dir",
            work / "cache" / f"116_GRCh38_{PROFILES[name].flavour}",
            "--output-dir",
            work,
        ]
        log_path = logs / f"{name}.log"
        print(f"[{index}/{len(args.profiles)}] {name}: {log_path}", flush=True)
        with log_path.open("w") as log:
            process = subprocess.run(
                list(map(str, command)), stdout=log, stderr=subprocess.STDOUT
            )
        row = profile_result(name, report_path, process.returncode, expected)
        rows.append(row)
        print(f"  {row['status']}: {name}", flush=True)
        if row["status"] != "PASS":
            print(
                "\n".join(log_path.read_text(errors="replace").splitlines()[-20:]),
                flush=True,
            )
        # The runner's plain reference slice is temporary; the BGZF golden and
        # annotated output remain available for independent inspection.
        plain_reference = (
            work / "results/116/fast_chr22" / f"vep_chr22_{PROFILES[name].suffix}.vcf"
        )
        plain_reference.unlink(missing_ok=True)
        with summary.open("w", newline="") as stream:
            writer = csv.DictWriter(
                stream,
                delimiter="\t",
                fieldnames=[
                    "profile",
                    "status",
                    "exit_code",
                    "records",
                    "csq_order_ignored",
                    "vep_strict_md5",
                    "vepyr_strict_md5",
                    "vep_canonical_md5",
                    "vepyr_canonical_md5",
                ],
            )
            writer.writeheader()
            writer.writerows(rows)
    passed = sum(row["status"] == "PASS" for row in rows)
    print(f"\n{passed}/{len(args.profiles)} profiles passed. Summary: {summary}")
    return 0 if passed == len(args.profiles) else 1


def main(argv=None):
    args = parse_args(argv)
    try:
        manifest = json.loads(MANIFEST.read_text())
        if set(manifest["profiles"]) != set(CORE_PROFILES):
            raise ValueError(
                "Golden manifest must contain exactly the ten core profiles"
            )
        for command in ("bcftools", "bgzip", "tabix"):
            if shutil.which(command) is None:
                raise RuntimeError(
                    f"{command} is required; use the reviewer Docker image"
                )
        args.work_dir.mkdir(parents=True, exist_ok=True)
        if not args.prepare_only:
            (args.work_dir / "summary.tsv").unlink(missing_ok=True)
        prepare_fixtures(manifest, args.profiles, args.offline)
        prepare_caches(manifest, args.profiles, args.work_dir, args.offline)
        if args.prepare_only:
            print("Inputs verified. Rerun without --prepare-only to compare.")
            return 0
        return compare_profiles(args, manifest)
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
